import asyncio
import dramatiq
import json

from aiofiles import os as aios
from diamond_miner.commons.database import Database
from diamond_miner.commons.redis import Redis
from diamond_miner.commons.storage import Storage
from diamond_miner.worker import logger
from diamond_miner.worker.processors import (
    pcap_to_csv,
    next_round_csv,
    shuffle_next_round_csv,
)
from diamond_miner.worker.settings import WorkerSettings


settings = WorkerSettings()
database = Database(host=settings.WORKER_DATABASE_HOST, logger=logger)
storage = Storage()


async def pipeline(
    agent_uuid,
    agent_parameters,
    measurement_parameters,
    result_filename,
    starttime_filename,
):
    """Process results and eventually request a new round."""
    logger.info(f"New files detected for agent `{agent_uuid}`")
    measurement_uuid = measurement_parameters["measurement_uuid"]
    timestamp = measurement_parameters["timestamp"]
    round_number = int(result_filename.split("_")[2].split(".")[0])

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

    logger.info(f"Create database `{settings.WORKER_DATABASE_NAME}`if not exists")
    await database.create_datebase(settings.WORKER_DATABASE_NAME)

    table_name = (
        settings.WORKER_DATABASE_NAME
        + "."
        + database.forge_table_name(measurement_uuid, agent_uuid, timestamp)
    )
    logger.info(f"Create table `{table_name}`")
    await database.create_table(table_name, drop=False)
    await database.clean_table(table_name)

    logger.info("Insert CSV file into database")
    await database.insert_csv(csv_filepath, table_name)

    if not settings.WORKER_DEBUG_MODE:
        logger.info("Remove local CSV file")
        await aios.remove(csv_filepath)

    next_round_number = round_number + 1
    next_round_csv_filename = f"{agent_uuid}_next_round_csv_{next_round_number}.csv"
    next_round_csv_filepath = str(measurement_results_path / next_round_csv_filename)

    if agent_parameters is None:
        logger.error("No agent parameters")

    logger.info("Compute the next round CSV file")
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

    logger.info("Shuffle next round CSV file")
    await shuffle_next_round_csv(
        next_round_csv_filepath, shuffled_next_round_csv_filepath
    )

    if not settings.WORKER_DEBUG_MODE:
        logger.info("Remove local next round CSV file")
        await aios.remove(next_round_csv_filepath)

    return (agent_uuid, shuffled_next_round_csv_filepath)


async def watch(agent_uuid, agent_parameters, measurement_parameters):
    """Watch for a results from an agent."""
    measurement_uuid = measurement_parameters["measurement_uuid"]
    while True:
        logger.debug(f"{measurement_uuid} -> {agent_uuid}")
        files = await storage.get_all_files(measurement_uuid)
        try:
            # TODO Check the round, take the lowest,
            # and check if it the same for the two files
            result_filename = [
                f["key"] for f in files if f["key"].startswith(f"{agent_uuid}_results")
            ][0]
            starttime_filename = [
                f["key"]
                for f in files
                if f["key"].startswith(f"{agent_uuid}_starttime")
            ][0]
            await pipeline(
                agent_uuid,
                agent_parameters,
                measurement_parameters,
                result_filename,
                starttime_filename,
            )
            break
        except IndexError:
            await asyncio.sleep(settings.WORKER_WATCH_REFRESH)


async def callback(agents, measurement_parameters):
    """Asynchronous callback."""
    logger.info("New measurement! Publishing request")
    measurement_uuid = measurement_parameters["measurement_uuid"]

    redis = Redis()
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    logger.info("Create local measurement directory if not exists")
    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid
    try:
        await aios.mkdir(str(measurement_results_path))
    except FileExistsError:
        logger.error(f"Local measurement `{measurement_uuid}` directory already exits.")
        return

    is_measurement_published = bool(await redis.get(f"published:{measurement_uuid}"))
    if not is_measurement_published:
        try:
            await storage.create_bucket(bucket=measurement_uuid)
        except Exception:
            logger.error(f"Impossible to create bucket `{measurement_uuid}`")
        await redis.set(f"published:{measurement_uuid}", "waiting")
        await redis.publish(
            "request:all",
            {
                "measurement_uuid": measurement_uuid,
                "measurement_tool": "diamond_miner",
                "timestamp": measurement_parameters["timestamp"],
                "round": 1,
                "parameters": dict(measurement_parameters),
            },
        )

    logger.info("Getting agent parameters")
    agents_parameters = []
    for agent_uuid in agents:
        agent_parameters = await redis.get(f"parameters:{agent_uuid}")
        if agent_parameters:
            agents_parameters.append(json.loads(agent_parameters))
        else:
            agents_parameters.append(None)

    logger.info("Watching ...")
    await asyncio.gather(
        *[
            watch(agent_uuid, agent_parameters, measurement_parameters)
            for agent_uuid, agent_parameters in zip(agents, agents_parameters)
            if agent_parameters is not None
        ]
    )

    if not settings.WORKER_DEBUG_MODE:
        logger.info("Removing local measurement directory")
        try:
            await aios.rmdir(str(measurement_results_path))
        except OSError:
            logger.error(
                f"Impossible to remove local measurement `{measurement_uuid}` directory"
            )

    logger.info("Delete measurement bucket")
    try:
        await storage.delete_bucket(bucket=measurement_uuid)
    except Exception:
        logger.error(f"Impossible to remove bucket `{measurement_uuid}`")

    await redis.delete(f"published:{measurement_uuid}")
    await redis.close()


@dramatiq.actor(
    time_limit=settings.WORKER_TIME_LIMIT, max_age=settings.WORKER_MESSAGE_AGE_LIMIT
)
def hook(agents, measurement_parameters):
    """Hook a worker process to a measurement"""
    asyncio.run(callback(agents, measurement_parameters))
