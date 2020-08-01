import asyncio
import dramatiq

from aiofiles import os as aios
from aiohttp.client_exceptions import ServerTimeoutError
from iris.commons.database import (
    DatabaseMeasurementResults,
    DatabaseMeasurements,
    DatabaseAgents,
    DatabaseAgentsInMeasurements,
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
from pathlib import Path


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
    database = DatabaseMeasurementResults(host=settings.DATABASE_HOST, logger=logger)

    measurement_uuid = measurement_parameters["measurement_uuid"]

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"
    logger.info(f"{logger_prefix} New files detected")

    round_number = extract_round_number(result_filename)
    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

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

    table_name = (
        settings.DATABASE_NAME
        + "."
        + database.forge_table_name(measurement_uuid, agent_uuid)
    )
    logger.info(f"{logger_prefix} Create table `{table_name}`")
    await database.create_table(table_name, drop=False)

    logger.info(f"{logger_prefix} Insert CSV file into database")
    await database.insert_csv(csv_filepath, table_name)

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local CSV file")
        await aios.remove(csv_filepath)

    next_round_number = round_number + 1
    next_round_csv_filename = f"{agent_uuid}_next_round_csv_{next_round_number}.csv"
    next_round_csv_filepath = str(measurement_results_path / next_round_csv_filename)

    if not agent_parameters:
        logger.error(f"{logger_prefix} No agent parameters")

    logger.info(f"{logger_prefix} Compute the next round CSV probe file")
    await next_round_csv(
        round_number,
        table_name,
        next_round_csv_filepath,
        agent_parameters,
        measurement_parameters,
        logger_prefx=logger_prefix + " ",
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

        logger.info(f"{logger_prefix} Uploading next round CSV probe file")
        with Path(shuffled_next_round_csv_filepath).open("rb") as fin:
            await storage.upload_file(
                measurement_uuid, shuffled_next_round_csv_filename, fin
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


async def watch(redis, agent_uuid, agent_parameters, measurement_parameters):
    """Watch for a results from an agent."""
    measurement_uuid = measurement_parameters["measurement_uuid"]
    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"
    while True:
        logger.debug(f"{logger_prefix} Active watching")

        if settings.WORKER_SANITY_CHECK_ENABLE:
            logger.debug(f"{measurement_uuid} :: {agent_uuid} :: Perform sanity check")
            is_agent_alive = await sanity_check(redis, measurement_uuid, agent_uuid)
            if not is_agent_alive:
                logger.warning(f"{logger_prefix} Stop watching agent, seems to be down")
                break

        try:
            remote_files = await storage.get_all_files(measurement_uuid)
        except ServerTimeoutError:
            logger.error(f"{logger_prefix} AWS Server timed out")
            await asyncio.sleep(settings.WORKER_WATCH_REFRESH)
            continue

        # Search for result file & start time file
        result_files = []
        starttime_files = []
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
            database_measurement_agents = DatabaseAgentsInMeasurements(
                host=settings.DATABASE_HOST,
                table_name=settings.AGENTS_IN_MEASUREMENTS_TABLE_NAME,
            )
            await database_measurement_agents.stamp_finished(
                measurement_uuid, agent_uuid
            )
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
                    "round": round_number,
                    "parameters": measurement_parameters,
                },
            )


async def callback(agents, measurement_parameters):
    """Asynchronous callback."""
    measurement_uuid = measurement_parameters["measurement_uuid"]
    user = measurement_parameters["user"]

    logger.info(f"{measurement_uuid} :: New measurement received")
    database_measurements = DatabaseMeasurements(
        host=settings.DATABASE_HOST, table_name=settings.MEASUREMENTS_TABLE_NAME,
    )
    database_agents = DatabaseAgents(
        host=settings.DATABASE_HOST, table_name=settings.AGENTS_TABLE_NAME,
    )
    database_measurement_agents = DatabaseAgentsInMeasurements(
        host=settings.DATABASE_HOST,
        table_name=settings.AGENTS_IN_MEASUREMENTS_TABLE_NAME,
    )

    redis = Redis()
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    logger.info(f"{measurement_uuid} :: Getting agents parameters")
    agents_parameters = {}
    for agent_uuid in agents:
        agent_parameters = await redis.get_agent_parameters(agent_uuid)
        if agent_parameters:
            agents_parameters[agent_uuid] = agent_parameters

    if not agents_parameters:
        logger.error(
            f"{measurement_uuid} :: Stopping measurement "
            "because no agent with parameters"
        )

    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    if await redis.get_measurement_state(measurement_uuid) is None:
        # There is no measurement state, so the measurement hasn't started yet
        logger.info(f"{measurement_uuid} :: Set measurement state to `waiting`")
        await redis.set_measurement_state(measurement_uuid, "waiting")

        logger.info(
            f"{measurement_uuid} :: Create local measurement directory if not exists"
        )
        try:
            await aios.mkdir(str(measurement_results_path))
        except FileExistsError:
            logger.warning(
                f"{measurement_uuid} :: Local measurement directory already exits"
            )

        logger.info(f"{measurement_uuid} :: Register measurement into database")
        await database_measurements.create_table()
        await database_measurements.register(agents, measurement_parameters)

        logger.info(f"{measurement_uuid} :: Register agents into database")
        await database_agents.create_table()
        await database_measurement_agents.create_table()
        for agent_uuid, agent_parameters in agents_parameters.items():
            # Register information about the physical agent
            is_already_present = await database_agents.get(agent_uuid)
            if is_already_present is None:
                # Physical agent not present, registering
                await database_agents.register(agent_uuid, agent_parameters)
            else:
                # Already present, updating last used
                await database_agents.stamp_last_used(agent_uuid)

            # Register information of agent specific of this measurement
            await database_measurement_agents.register(
                measurement_uuid,
                agent_uuid,
                min_ttl=measurement_parameters["min_ttl"],
                max_ttl=measurement_parameters["max_ttl"],
            )

        logger.info(f"{measurement_uuid} :: Create bucket in AWS S3")
        try:
            await storage.create_bucket(bucket=measurement_uuid)
        except Exception:
            logger.error(f"{measurement_uuid} :: Impossible to create bucket")
            return

        logger.info(f"{measurement_uuid} :: Publish measurement to agents")
        measurement_request = {
            "measurement_uuid": measurement_uuid,
            "measurement_tool": "iris",
            "round": 1,
            "parameters": measurement_parameters,
        }
        if not measurement_parameters["agents"]:
            await redis.publish("all", measurement_request)
        else:
            agents = measurement_parameters["agents"]
            for agent_uuid in measurement_parameters["agents"]:
                await redis.publish(agent_uuid, measurement_request)
    else:
        # We are in this state when the worker has fail and replaying the measurement
        # So we stip off the agents those which are finished
        agent_in_measurement_info = await database_measurement_agents.all(
            measurement_uuid
        )
        finished_agents = [
            a["uuid"] for a in agent_in_measurement_info if a["state"] == "finished"
        ]
        filtered_agents_parameters = {}
        for agent_uuid, agent_parameters in agents_parameters.items():
            if agent_uuid not in finished_agents:
                filtered_agents_parameters[agent_uuid] = agent_parameters
        agents_parameters = filtered_agents_parameters

    logger.info(f"{measurement_uuid} :: Watching ...")
    await asyncio.gather(
        *[
            watch(redis, agent_uuid, agent_parameters, measurement_parameters)
            for agent_uuid, agent_parameters in agents_parameters.items()
        ]
    )

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{measurement_uuid} :: Removing local measurement directory")
        try:
            await aios.rmdir(str(measurement_results_path))
        except OSError:
            logger.error(
                f"{measurement_uuid} :: "
                "Impossible to remove local measurement directory"
            )

    logger.info(f"{measurement_uuid} :: Delete bucket")
    try:
        await storage.delete_bucket(bucket=measurement_uuid)
    except Exception:
        logger.error(f"{measurement_uuid} :: Impossible to remove bucket")

    logger.info(f"{measurement_uuid} :: Stamp the end time for measurement")
    await database_measurements.stamp_end_time(user, measurement_uuid)

    logger.info(f"{measurement_uuid} :: Measurement done")
    await redis.delete_measurement_state(measurement_uuid)
    await redis.close()


@dramatiq.actor(
    time_limit=settings.WORKER_TIME_LIMIT, max_age=settings.WORKER_MESSAGE_AGE_LIMIT
)
def hook(agents, measurement_parameters):
    """Hook a worker process to a measurement"""
    asyncio.run(callback(agents, measurement_parameters))
