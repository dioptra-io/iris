"""Diamond-Miner pipeline."""

import asyncio
import ipaddress
import ssl

from aiofiles import os as aios
from concurrent.futures import ProcessPoolExecutor
from diamond_miner_core import (
    compute_next_round,
    MeasurementParameters,
    HeidemannFlowMapper,
)

from iris.commons.database import get_session, DatabaseMeasurementResults
from iris.commons.storage import Storage
from iris.worker import logger
from iris.worker.processors import shuffle_next_round_csv
from iris.worker.settings import WorkerSettings


settings = WorkerSettings()
settings_redis_ssl = ssl.SSLContext() if settings.REDIS_SSL else None
storage = Storage()


def extract_round_number(filename):
    return int(filename.split("_")[-1].split(".")[0])


async def diamond_miner_pipeline(parameters, result_filename):
    """Process results and eventually request a new round."""
    measurement_uuid = parameters.measurement_uuid
    agent_uuid = parameters.agent_uuid

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"
    logger.info(f"{logger_prefix} New files detected")

    round_number = extract_round_number(result_filename)
    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    logger.info(f"{logger_prefix} Round {round_number}")
    logger.info(f"{logger_prefix} Download results file")
    results_filepath = str(measurement_results_path / result_filename)
    await storage.download_file(measurement_uuid, result_filename, results_filepath)

    logger.info(f"{logger_prefix} Delete results file from AWS S3")
    response = await storage.delete_file_no_check(measurement_uuid, result_filename)
    if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
        logger.error(f"Impossible to remove result file `{result_filename}`")

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
    await database.insert_csv(results_filepath)

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local CSV file")
        await aios.remove(results_filepath)

    # HACK: Temporary remove this feature to avoid the following botocore issue.
    #         An error occurred (NoSuchKey) when calling the GetObject operation:
    #         The specified key does not exist.
    # If the targets_file_key is `targets-list`, then the max round is 1
    # if parameters.targets_file_key is not None:
    #     targets_info = await storage.get_file(
    #         settings.AWS_S3_TARGETS_BUCKET_PREFIX + parameters.user,
    #         parameters.targets_file_key,
    #     )
    #     targets_type = targets_info.get("metadata", {}).get("type", "targets-list")
    #     if targets_type == "targets-list":
    #         logger.info(
    #             f"{logger_prefix} Maximum round reached for `targets-list`. Stopping."
    #         )
    #         return None

    next_round_number = round_number + 1
    next_round_csv_filename = f"{agent_uuid}_next_round_csv_{next_round_number}.csv"
    next_round_csv_filepath = str(measurement_results_path / next_round_csv_filename)

    if next_round_number > parameters.max_round:
        logger.info(f"{logger_prefix} Maximum round reached. Stopping.")
        return None

    logger.info(f"{logger_prefix} Compute the next round CSV probe file")
    # TODO Rewrite the lib in an asynchonous way
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(
        ProcessPoolExecutor(),
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
        HeidemannFlowMapper(),
        False,  # No max-ttl exploration feature
        True,  # Skip unpopulated TTLs
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
