"""Diamond-Miner pipeline."""

import asyncio
import ipaddress
import ssl

from aiofiles import os as aios
from diamond_miner_core import (
    compute_next_round,
    MeasurementParameters,
    RandomFlowMapper,
)

from iris.commons.database import get_session, DatabaseMeasurementResults
from iris.commons.storage import Storage
from iris.worker import logger
from iris.worker.processors import pcap_to_csv, shuffle_next_round_csv
from iris.worker.settings import WorkerSettings


settings = WorkerSettings()
settings_redis_ssl = ssl.SSLContext() if settings.REDIS_SSL else None
storage = Storage()


def extract_round_number(filename):
    return int(filename.split("_")[-1].split(".")[0])


async def diamond_miner_pipeline(
    parameters, result_filename, starttime_filename,
):
    """Process results and eventually request a new round."""
    measurement_uuid = parameters.measurement_uuid
    agent_uuid = parameters.agent_uuid

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"
    logger.info(f"{logger_prefix} New files detected")

    round_number = extract_round_number(result_filename)

    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    logger.info(f"{logger_prefix} Round {round_number}")
    logger.info(f"{logger_prefix} Download results file & start time log file")
    result_filepath = str(measurement_results_path / result_filename)
    await storage.download_file(measurement_uuid, result_filename, result_filepath)
    starttime_filepath = str(measurement_results_path / starttime_filename)
    await storage.download_file(
        measurement_uuid, starttime_filename, starttime_filepath
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
        parameters.destination_port,
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

    if next_round_number > parameters.max_round:
        logger.warning(f"{logger_prefix} Maximum round reached. Stopping.")
        return None

    logger.info(f"{logger_prefix} Compute the next round CSV probe file")
    # TODO MRe-write the lib in an asynchonous way
    # TODO Excluded prefixes file
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        None,
        compute_next_round,
        settings.DATABASE_HOST,
        table_name,
        MeasurementParameters(
            source_ip=int(ipaddress.IPv4Address(parameters.ip_address)),
            source_port=24000,  # TODO Put in measurement parameters ?
            destination_port=parameters.destination_port,
            min_ttl=parameters.min_ttl,
            max_ttl=parameters.max_ttl,
            round_number=round_number,
        ),
        next_round_csv_filepath,
        RandomFlowMapper(parameters.seed, n_array=1000),
        False,
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
