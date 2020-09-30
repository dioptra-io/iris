import asyncio
import dramatiq
import traceback

from aiofiles import os as aios
from iris.commons.database import (
    get_session,
    DatabaseMeasurementResults,
    DatabaseMeasurements,
    DatabaseAgents,
    DatabaseAgentsSpecific,
)
from iris.commons.redis import Redis
from iris.commons.storage import Storage
from iris.worker import logger
from iris.worker.processors import (
    pcap_to_csv,
    next_round_csv,
    shuffle_next_round_csv,
)
from iris.worker.settings import WorkerSettings


settings = WorkerSettings()
storage = Storage()


def extract_round_number(filename):
    return int(filename.split("_")[-1].split(".")[0])


async def pipeline(
    agent_uuid,
    agent_parameters,
    measurement_parameters,
    result_filename,
    starttime_filename,
):
    """Process results and eventually request a new round."""
    measurement_uuid = measurement_parameters["measurement_uuid"]

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"
    logger.info(f"{logger_prefix} New files detected")

    round_number = extract_round_number(result_filename)
    max_round = measurement_parameters["max_round"]
    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    logger.info(f"{logger_prefix} Round {round_number}")
    logger.info(f"{logger_prefix} Download results file & start time log file")
    result_filepath = str(measurement_results_path / result_filename)
    await storage.download_file(
        measurement_uuid, result_filename, result_filepath,
    )
    starttime_filepath = str(measurement_results_path / starttime_filename)
    await storage.download_file(
        measurement_uuid, starttime_filename, starttime_filepath,
    )

    logger.info(
        f"{logger_prefix} Transform results file & start time log file into CSV file"
    )
    csv_filename = f"{agent_uuid}_csv_{round_number}.csv"
    csv_filepath = str(measurement_results_path / csv_filename)
    await pcap_to_csv(
        round_number,
        result_filepath,
        starttime_filepath,
        csv_filepath,
        measurement_parameters,
        logger_prefix=logger_prefix + " ",
    )

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local results file & start time log file")
        await aios.remove(result_filepath)
        await aios.remove(starttime_filepath)

    logger.info(
        f"{logger_prefix} Delete results file & start time log file from AWS S3"
    )
    response = await storage.delete_file_no_check(measurement_uuid, result_filename)
    if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        logger.error(f"Impossible to remove result file `{result_filename}`")
    response = await storage.delete_file_no_check(measurement_uuid, starttime_filename)
    if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        logger.error(f"Impossible to remove result file `{starttime_filename}`")

    session = get_session()
    table_name = (
        settings.DATABASE_NAME
        + "."
        + DatabaseMeasurementResults.forge_table_name(measurement_uuid, agent_uuid)
    )
    database = DatabaseMeasurementResults(session, table_name, logger=logger)

    logger.info(f"{logger_prefix} Create table `{table_name}`")
    await database.create_table(drop=False)

    logger.info(f"{logger_prefix} Insert CSV file into database")
    await database.insert_csv(csv_filepath)

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local CSV file")
        await aios.remove(csv_filepath)

    next_round_number = round_number + 1
    next_round_csv_filename = f"{agent_uuid}_next_round_csv_{next_round_number}.csv"
    next_round_csv_filepath = str(measurement_results_path / next_round_csv_filename)

    if next_round_number > max_round:
        logger.warning(f"{logger_prefix} Maximum round reached. Stopping.")
        return None

    if not agent_parameters:
        logger.error(f"{logger_prefix} No agent parameters")

    logger.info(f"{logger_prefix} Compute the next round CSV probe file")
    await next_round_csv(
        round_number,
        table_name,
        next_round_csv_filepath,
        agent_parameters,
        measurement_parameters,
        logger_prefix=logger_prefix + " ",
    )

    shuffled_next_round_csv_filename = (
        f"{agent_uuid}_shuffled_next_round_csv_{next_round_number}.csv"
    )
    shuffled_next_round_csv_filepath = str(
        measurement_results_path / shuffled_next_round_csv_filename
    )

    if (await aios.stat(next_round_csv_filepath)).st_size != 0:
        logger.info(f"{logger_prefix} Next round is required")
        logger.info(f"{logger_prefix} Shuffle next round CSV probe file")
        await shuffle_next_round_csv(
            next_round_csv_filepath,
            shuffled_next_round_csv_filepath,
            logger_prefix=logger_prefix + " ",
        )

        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local next round CSV probe file")
            await aios.remove(next_round_csv_filepath)

        logger.info(f"{logger_prefix} Uploading shuffled next round CSV probe file")
        await storage.upload_file(
            measurement_uuid,
            shuffled_next_round_csv_filename,
            shuffled_next_round_csv_filepath,
        )

        if not settings.WORKER_DEBUG_MODE:
            logger.info(
                f"{logger_prefix} Remove local shuffled next round CSV probe file"
            )
            await aios.remove(shuffled_next_round_csv_filepath)
        return shuffled_next_round_csv_filename

    else:
        logger.info(f"{logger_prefix} Next round is not required")
        if not settings.WORKER_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local empty next round CSV probe file")
            await aios.remove(next_round_csv_filepath)
        return None


async def sanity_clean(measurement_uuid, agent_uuid):
    """Clean AWS S3 if the sanity check don't pass."""
    remote_files = await storage.get_all_files(measurement_uuid)
    for remote_file in remote_files:
        remote_filename = remote_file["key"]
        if remote_filename.startswith(agent_uuid):
            logger.warning(f"Sanity remove `{remote_filename}` from AWS S3")
            response = await storage.delete_file_no_check(
                measurement_uuid, remote_filename
            )
            if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
                logger.error(f"Impossible to remove `{remote_filename}`")


async def sanity_check(redis, measurement_uuid, agent_uuid):
    """
    Sanity check to close the loop if the agent is disconnected.
    Returns: True if the agent is alive, else False.
    """
    checks = []
    for _ in range(settings.WORKER_SANITY_CHECK_RETRIES):
        checks.append(await redis.check_agent(agent_uuid))
        await asyncio.sleep(settings.WORKER_SANITY_CHECK_REFRESH)
    if False in checks:
        await sanity_clean(measurement_uuid, agent_uuid)
        return False
    return True


async def watch(
    redis, agent_uuid, agent_parameters, measurement_parameters, specific_parameters
):
    """Watch for a results from an agent."""
    measurement_uuid = measurement_parameters["measurement_uuid"]
    username = measurement_parameters["user"]

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"

    session = get_session()
    database_agents_specific = DatabaseAgentsSpecific(session)

    while True:
        if settings.WORKER_SANITY_CHECK_ENABLE:
            # Check if the agent is down
            is_agent_alive = await sanity_check(redis, measurement_uuid, agent_uuid)
            if not is_agent_alive:
                logger.warning(f"{logger_prefix} Stop watching agent")
                await database_agents_specific.stamp_finished(
                    measurement_uuid, agent_uuid
                )
                break
            # Check if the measurement has been canceled
            measurement_state = await redis.get_measurement_state(measurement_uuid)
            if measurement_state is None:
                logger.warning(f"{logger_prefix} Measurement canceled")
                await database_agents_specific.stamp_finished(
                    measurement_uuid, agent_uuid
                )
                break

        # Search for result file & start time file
        result_files = []
        starttime_files = []
        remote_files = await storage.get_all_files(measurement_uuid)
        for remote_file in remote_files:
            remote_filename = remote_file["key"]
            if remote_filename.startswith(f"{agent_uuid}_results"):
                result_files.append(remote_filename)
            elif remote_filename.startswith(f"{agent_uuid}_starttime"):
                starttime_files.append(remote_filename)

        if len(result_files) == 0 or len(starttime_files) == 0:
            # The result file & start time file are not present, watch again
            await asyncio.sleep(settings.WORKER_WATCH_REFRESH)
            continue

        result_files.sort(key=lambda x: extract_round_number(x))
        starttime_files.sort(key=lambda x: extract_round_number(x))

        lowest_round_result_files = extract_round_number(result_files[0])
        lowest_round_starttime_files = extract_round_number(starttime_files[0])

        if lowest_round_result_files != lowest_round_starttime_files:
            # The lowest round numbers don't match, watch again
            await asyncio.sleep(settings.WORKER_WATCH_REFRESH)
            continue

        result_filename, starttime_filename = result_files[0], starttime_files[0]

        # If found, then execute process pipeline
        shuffled_next_round_csv_filepath = await pipeline(
            agent_uuid,
            agent_parameters,
            measurement_parameters,
            result_filename,
            starttime_filename,
        )

        if shuffled_next_round_csv_filepath is None:
            logger.info(f"{logger_prefix} Measurement done for this agent")
            await database_agents_specific.stamp_finished(measurement_uuid, agent_uuid)
            break
        else:
            logger.info(f"{logger_prefix} Publish next mesurement")

            round_number = int(
                shuffled_next_round_csv_filepath.split("_")[-1].split(".")[0]
            )
            measurement_parameters["csv_probe_file"] = shuffled_next_round_csv_filepath

            await redis.publish(
                agent_uuid,
                {
                    "measurement_uuid": measurement_uuid,
                    "measurement_tool": "diamond_miner",
                    "username": username,
                    "round": round_number,
                    "specific": specific_parameters,
                    "parameters": measurement_parameters,
                },
            )


async def callback(agents, measurement_parameters):
    """Asynchronous callback."""
    measurement_uuid = measurement_parameters["measurement_uuid"]
    username = measurement_parameters["user"]

    logger_prefix = f"{measurement_uuid} ::"

    logger.info(f"{logger_prefix} New measurement received")
    session = get_session()
    database_measurements = DatabaseMeasurements(session)
    database_agents = DatabaseAgents(session)
    database_agents_specific = DatabaseAgentsSpecific(session)

    redis = Redis()
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    logger.info(f"{logger_prefix} Getting agents parameters")
    agents_parameters = {}
    for agent_uuid in agents:
        agent_parameters = await redis.get_agent_parameters(agent_uuid)
        if agent_parameters:
            agents_parameters[agent_uuid] = agent_parameters

    # Filter out agents that don't have physical parameters
    agents = {
        uuid: specific for uuid, specific in agents.items() if uuid in agents_parameters
    }

    if not agents:
        logger.error(
            f"{logger_prefix} Stopping measurement because no agent with parameters"
        )

    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    if await redis.get_measurement_state(measurement_uuid) is None:
        # There is no measurement state, so the measurement hasn't started yet
        logger.info(f"{logger_prefix} Set measurement state to `waiting`")
        await redis.set_measurement_state(measurement_uuid, "waiting")

        logger.info(f"{logger_prefix} Create local measurement directory")
        try:
            await aios.mkdir(str(measurement_results_path))
        except FileExistsError:
            logger.warning(f"{logger_prefix} Local measurement directory already exits")

        logger.info(f"{logger_prefix} Register measurement into database")
        await database_measurements.create_table()
        await database_measurements.register(measurement_parameters)

        logger.info(f"{logger_prefix} Register agents into database")
        await database_agents.create_table()
        await database_agents_specific.create_table()
        for agent_uuid, specific in agents.items():
            # Register information about the physical agent
            is_already_present = await database_agents.get(agent_uuid)
            if is_already_present is None:
                # Physical agent not present, registering
                await database_agents.register(
                    agent_uuid, agents_parameters[agent_uuid]
                )
            else:
                # Already present, updating last used
                await database_agents.stamp_last_used(agent_uuid)

            # Register agent in this measurement and specific information
            if not specific:
                specific = measurement_parameters.copy()
            if "probing_rate" not in specific or specific["probing_rate"] is None:
                specific["probing_rate"] = agents_parameters[agent_uuid]["probing_rate"]
            await database_agents_specific.register(
                measurement_uuid, agent_uuid, specific
            )

        logger.info(f"{logger_prefix} Create bucket in AWS S3")
        try:
            await storage.create_bucket(bucket=measurement_uuid)
        except Exception:
            logger.error(f"{logger_prefix} Impossible to create bucket")
            return

        logger.info(f"{logger_prefix} Publish measurement to agents")
        request = {
            "measurement_uuid": measurement_uuid,
            "measurement_tool": "diamond-miner",
            "username": username,
            "round": 1,
            "specific": {},
            "parameters": measurement_parameters,
        }

        if not all(agents.values()):
            # If no agent specific parameters
            await redis.publish("all", request)
        else:
            # Else, append specific parameter by agent
            for agent_uuid, specific in agents.items():
                request["specific"] = specific
                await redis.publish(agent_uuid, request)
    else:
        # We are in this state when the worker has fail and replaying the measurement
        # So we stip off the agents those which are finished
        agent_specific_info = await database_agents_specific.all(measurement_uuid)
        finished_agents = [
            a["uuid"] for a in agent_specific_info if a["state"] == "finished"
        ]
        filtered_agents_parameters = {}
        for agent_uuid, agent_parameters in agents_parameters.items():
            if agent_uuid not in finished_agents:
                filtered_agents_parameters[agent_uuid] = agent_parameters
        agents_parameters = filtered_agents_parameters

    logger.info(f"{logger_prefix} Watching ...")
    await asyncio.gather(
        *[
            watch(
                redis,
                agent_uuid,
                agent_parameters,
                measurement_parameters,
                agents[agent_uuid],
            )
            for agent_uuid, agent_parameters in agents_parameters.items()
        ]
    )

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Removing local measurement directory")
        try:
            await aios.rmdir(str(measurement_results_path))
        except OSError:
            logger.error(
                f"{logger_prefix} Impossible to remove local measurement directory"
            )

    logger.info(f"{logger_prefix} Delete bucket")
    try:
        await storage.delete_bucket(bucket=measurement_uuid)
    except Exception:
        logger.error(f"{logger_prefix} Impossible to remove bucket")

    logger.info(f"{logger_prefix} Stamp the end time for measurement")
    await database_measurements.stamp_end_time(username, measurement_uuid)

    logger.info(f"{logger_prefix} Measurement done")
    await redis.delete_measurement_state(measurement_uuid)
    await redis.disconnect()


@dramatiq.actor(
    time_limit=settings.WORKER_TIME_LIMIT, max_age=settings.WORKER_MESSAGE_AGE_LIMIT
)
def hook(agents_specific, measurement_parameters):
    """Hook a worker process to a measurement"""
    try:
        asyncio.run(callback(agents_specific, measurement_parameters))
    except Exception as exception:
        measurement_uuid = measurement_parameters["measurement_uuid"]
        traceback_content = traceback.format_exc()
        for line in traceback_content.splitlines():
            logger.critical(f"{measurement_uuid} :: {line}")
        raise exception
