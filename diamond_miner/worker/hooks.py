import asyncio
import dramatiq

from aiofiles import os as aios
from diamond_miner.commons.storage import Storage
from diamond_miner.worker import logger
from diamond_miner.worker.settings import WorkerSettings
from diamond_miner.worker.processors import pcap_to_csv


settings = WorkerSettings()
storage = Storage()


async def pipeline(
    measurement_uuid, agent_uuid, parameters, result_filename, starttime_filename
):
    """Process results and eventually request a new round."""
    round_number = result_filename.split("_")[2].split(".")[0]

    logger.info("Download results file & start time log file")
    result_filepath = str(settings.WORKER_RESULTS_DIR / result_filename)
    await storage.download_file(
        measurement_uuid, result_filename, result_filepath,
    )
    starttime_filepath = str(settings.WORKER_RESULTS_DIR / starttime_filename)
    await storage.download_file(
        measurement_uuid, starttime_filename, starttime_filepath,
    )

    logger.info("Transform results file & start time log file into CSV file")
    output_csv_filename = f"{agent_uuid}_csv_{round_number}.csv"
    output_csv_filepath = str(settings.WORKER_RESULTS_DIR / output_csv_filename)
    await pcap_to_csv(
        round_number,
        result_filepath,
        starttime_filepath,
        output_csv_filepath,
        parameters,
    )

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


async def watch(measurement_uuid, agent, parameters):
    """Watch for a results from an agent."""
    agent_uuid = agent[3]
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
                measurement_uuid,
                agent_uuid,
                parameters,
                result_filename,
                starttime_filename,
            )
            break
        except IndexError:
            await asyncio.sleep(settings.WORKER_WATCH_REFRESH)


async def callback(measurement_uuid, agents, parameters):
    """Asynchronous callback."""
    logger.info("New result file detected! Processing ...")
    await asyncio.gather(
        *[watch(measurement_uuid, agent, parameters) for agent in agents]
    )
    logger.info("Delete measurement bucket")
    try:
        await storage.delete_bucket(bucket=measurement_uuid)
    except Exception:
        logger.error(f"Impossible to remove bucket `{measurement_uuid}`")
    return True


@dramatiq.actor(time_limit=settings.WORKER_TIMEOUT)
def hook(measurement_uuid, agents, parameters):
    """Hook a worker process to a measurement"""
    asyncio.run(callback(measurement_uuid, agents, parameters))
