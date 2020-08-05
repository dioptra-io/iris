"""Measurement interface."""

from aiofiles import os as aios
from iris.agent import logger
from iris.agent.prober import probe
from iris.agent.settings import AgentSettings
from iris.commons.storage import Storage
from pathlib import Path


settings = AgentSettings()
storage = Storage()


async def measuremement(uuid, request):
    """Conduct a measurement."""
    # Lock the client state
    measurement_uuid = request["measurement_uuid"]
    round_number = request["round"]

    min_ttl = request["parameters"]["min_ttl"]
    max_ttl = request["parameters"]["max_ttl"]

    logger_prefix = f"{measurement_uuid} :: {uuid} ::"

    measurement_results_path = settings.AGENT_RESULTS_DIR_PATH / measurement_uuid
    logger.info(f"{logger_prefix} Create local measurement directory")
    try:
        await aios.mkdir(str(measurement_results_path))
    except FileExistsError:
        logger.warning(f"{logger_prefix} Local measurement directory already exits")

    result_filename = f"{uuid}_results_{round_number}.pcap"
    result_filepath = str(measurement_results_path / result_filename)
    starttime_filename = f"{uuid}_starttime_{round_number}.log"
    starttime_filepath = str(measurement_results_path / starttime_filename)

    logger.info(f"{logger_prefix} Round : {round_number}")
    logger.info(f"{logger_prefix} Minimum TTL : {min_ttl}")
    logger.info(f"{logger_prefix} Maximum TTL : {max_ttl}")

    if round_number == 1:
        if request["parameters"]["full"]:
            logger.info(f"{logger_prefix} Full snapshot required")
            target_filepath = None
        else:
            logger.info(f"{logger_prefix} Target file based snapshot required")
            logger.info(f"{logger_prefix} Download target file locally")
            target_filename = request["parameters"]["targets_file_key"]
            target_filepath = str(settings.AGENT_TARGETS_DIR_PATH / target_filename)
            await storage.download_file(
                settings.AWS_S3_TARGETS_BUCKET_NAME, target_filename, target_filepath
            )
        csv_filepath = None
    else:
        logger.info(f"{logger_prefix} Download CSV probe file locally")
        target_filepath = None
        csv_filename = request["parameters"]["csv_probe_file"]
        csv_filepath = str(settings.AGENT_TARGETS_DIR_PATH / csv_filename)
        await storage.download_file(measurement_uuid, csv_filename, csv_filepath)

    logger.info(f"{logger_prefix} Starting Dimond-miner measurement")
    await probe(
        request,
        result_filepath,
        starttime_filepath,
        target_filepath=target_filepath,
        csv_filepath=csv_filepath,
        logger_prefix=logger_prefix + " ",
    )

    logger.info(f"{logger_prefix} Upload result file & start time log file into AWS S3")
    with Path(result_filepath).open("rb") as fin:
        await storage.upload_file(measurement_uuid, result_filename, fin)
    with Path(starttime_filepath).open("rb") as fin:
        await storage.upload_file(measurement_uuid, starttime_filename, fin)

    if not settings.AGENT_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local result file & start time log file")
        await aios.remove(result_filepath)
        await aios.remove(starttime_filepath)

    if not settings.AGENT_DEBUG_MODE:
        logger.info(f"{logger_prefix} Removing local measurement directory")
        try:
            await aios.rmdir(str(measurement_results_path))
        except OSError:
            logger.error(
                f"{logger_prefix} Impossible to remove local measurement directory"
            )

    if target_filepath is not None:
        logger.info(f"{logger_prefix} Remove local target file")
        await aios.remove(target_filepath)
    if csv_filepath is not None and not settings.AGENT_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local CSV probe file")
        await aios.remove(csv_filepath)

    if csv_filepath is not None:
        logger.info(f"{logger_prefix} Remove CSV probe file from AWS S3")
        response = await storage.delete_file_no_check(measurement_uuid, csv_filename)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
            logger.error(f"Impossible to remove result file `{csv_filename}`")
