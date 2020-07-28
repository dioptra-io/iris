import asyncio
import dramatiq

from aiofiles import os as aios
from datetime import datetime
from iris.commons.database import (
    DatabaseMeasurementResults,
    DatabaseMeasurements,
    DatabaseAgents,
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

    logger.info(f"New files detected for agent `{agent_uuid}`")
    measurement_uuid = measurement_parameters["measurement_uuid"]
    round_number = extract_round_number(result_filename)

    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    logger.info("Download results file & start time log file")
    result_filepath = str(measurement_results_path / result_filename)
    await storage.download_file(
        measurement_uuid, result_filename, result_filepath,
    )
    starttime_filepath = str(measurement_results_path / starttime_filename)
    await storage.download_file(
        measurement_uuid, starttime_filename, starttime_filepath,
    )

    logger.info("Transform results file & start time log file into CSV file")
    csv_filename = f"{agent_uuid}_csv_{round_number}.csv"
    csv_filepath = str(measurement_results_path / csv_filename)
    await pcap_to_csv(
        round_number,
        result_filepath,
        starttime_filepath,
        csv_filepath,
        measurement_parameters,
    )

    if not settings.WORKER_DEBUG_MODE:
        logger.info("Remove local results file & start time log file")
        await aios.remove(result_filepath)
        await aios.remove(starttime_filepath)

    logger.info("Delete results file & start time log file from AWS S3")
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
    logger.info(f"Create table `{table_name}`")
    await database.create_table(table_name, drop=False)

    logger.info("Insert CSV file into database")
    await database.insert_csv(csv_filepath, table_name)

    if not settings.WORKER_DEBUG_MODE:
        logger.info("Remove local CSV file")
        await aios.remove(csv_filepath)

    next_round_number = round_number + 1
    next_round_csv_filename = f"{agent_uuid}_next_round_csv_{next_round_number}.csv"
    next_round_csv_filepath = str(measurement_results_path / next_round_csv_filename)

    if not agent_parameters:
        logger.error("No agent parameters")

    logger.info("Compute the next round CSV probe file")
    await next_round_csv(
        round_number,
        table_name,
        next_round_csv_filepath,
        agent_parameters,
        measurement_parameters,
    )

    shuffled_next_round_csv_filename = (
        f"{agent_uuid}_shuffled_next_round_csv_{next_round_number}.csv"
    )
    shuffled_next_round_csv_filepath = str(
        measurement_results_path / shuffled_next_round_csv_filename
    )

    if not settings.WORKER_DEBUG_MODE:
        logger.info("Remove local next round CSV probe file")
        await aios.remove(next_round_csv_filepath)

    if (await aios.stat(next_round_csv_filepath)).st_size != 0:
        logger.info(f"Next round is required for measurement `{measurement_uuid}`")
        logger.info("Shuffle next round CSV probe file")
        await shuffle_next_round_csv(
            next_round_csv_filepath, shuffled_next_round_csv_filepath
        )

        logger.info("Uploading next round CSV probe file")
        with Path(shuffled_next_round_csv_filepath).open("rb") as fin:
            await storage.upload_file(
                measurement_uuid, shuffled_next_round_csv_filename, fin
            )

        if not settings.WORKER_DEBUG_MODE:
            logger.info("Remove local next round CSV probe file")
            await aios.remove(shuffled_next_round_csv_filepath)
        return shuffled_next_round_csv_filename
    else:
        logger.info(f"Next round is not required for measurement `{measurement_uuid}`")
        if not settings.WORKER_DEBUG_MODE:
            logger.info("Remove local empty next round CSV probe file")
            await aios.remove(shuffled_next_round_csv_filepath)
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
    while True:
        logger.debug(f"{measurement_uuid} -> {agent_uuid}")

        if settings.WORKER_SANITY_CHECK_ENABLE:
            logger.debug(f"Perform sanity check on agent `{agent_uuid}`")
            is_agent_alive = await sanity_check(redis, measurement_uuid, agent_uuid)
            if not is_agent_alive:
                logger.warning(f"Agent `{agent_uuid} seems to be down. Stop watching.`")
                break

        remote_files = await storage.get_all_files(measurement_uuid)

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

        sorted(result_files, key=lambda x: extract_round_number(x))
        sorted(starttime_files, key=lambda x: extract_round_number(x))

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
            logger.info(
                f"Measurement `{measurement_uuid}` is done for agent `{agent_uuid}`"
            )
            break
        else:
            logger.info(
                f"Publish measurement `{measurement_uuid}` to agent {agent_uuid}"
            )

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

    logger.info(f"New measurement `{measurement_uuid}` received")
    database_measurements = DatabaseMeasurements(
        host=settings.DATABASE_HOST, table_name=settings.MEASUREMENTS_TABLE_NAME,
    )
    database_agents = DatabaseAgents(
        host=settings.DATABASE_HOST, table_name=settings.AGENTS_TABLE_NAME,
    )

    redis = Redis()
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    logger.info("Getting agents parameters")
    agents_parameters = {}
    for agent_uuid in agents:
        agent_parameters = await redis.get_agent_parameters(agent_uuid)
        if agent_parameters:
            agents_parameters[agent_uuid] = agent_parameters

    if await redis.get_measurement_state(measurement_uuid) is None:
        # There is no measurement state, so the measurement hasn't started yet
        logger.info(f"Set measurement `{measurement_uuid}` state to `waiting`")
        await redis.set_measurement_state(measurement_uuid, "waiting")

        logger.info("Create local measurement directory if not exists")
        measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid
        try:
            await aios.mkdir(str(measurement_results_path))
        except FileExistsError:
            logger.warning(
                f"Local measurement `{measurement_uuid}` directory already exits"
            )

        logger.info(f"Register measurement `{measurement_uuid}` into database")
        await database_measurements.create_table()
        await database_measurements.register(agents, measurement_parameters)

        logger.info("Register agents into database")
        await database_agents.create_table()
        for agent_uuid, agent_parameters in agents_parameters.items():
            is_already_present = await database_agents.get(agent_uuid)
            if is_already_present is None:
                await database_agents.register(agent_uuid, agent_parameters)

        logger.info(f"Create measurement bucket  `{measurement_uuid}` in AWS S3")
        try:
            await storage.create_bucket(bucket=measurement_uuid)
        except Exception:
            logger.error(f"Impossible to create bucket `{measurement_uuid}`")
            return

        logger.info(f"Publish measurement `{measurement_uuid}` to agents")
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

    logger.info("Watching ...")
    await asyncio.gather(
        *[
            watch(redis, agent_uuid, agent_parameters, measurement_parameters)
            for agent_uuid, agent_parameters in agents_parameters.items()
        ]
    )

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"Removing local measurement directory `{measurement_uuid}`")
        try:
            await aios.rmdir(str(measurement_results_path))
        except OSError:
            logger.error(
                f"Impossible to remove local measurement `{measurement_uuid}` directory"
            )

    logger.info(f"Delete measurement bucket `{measurement_uuid}`")
    try:
        await storage.delete_bucket(bucket=measurement_uuid)
    except Exception:
        logger.error(f"Impossible to remove bucket `{measurement_uuid}`")

    logger.info(f"Stamp the end time for measurement `{measurement_uuid}`")
    await database_measurements.stamp_end_time(user, measurement_uuid, datetime.now())

    logger.info(f"Measurement `{measurement_uuid}` is done")
    await redis.delete_measurement_state(measurement_uuid)
    await redis.close()


@dramatiq.actor(
    time_limit=settings.WORKER_TIME_LIMIT, max_age=settings.WORKER_MESSAGE_AGE_LIMIT
)
def hook(agents, measurement_parameters):
    """Hook a worker process to a measurement"""
    asyncio.run(callback(agents, measurement_parameters))
