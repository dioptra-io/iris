"""Measurement interface."""

from aiofiles import os as aios
from iris.agent import logger
from iris.agent.prober import probe, stopper
from iris.agent.settings import AgentSettings
from iris.commons.storage import Storage


settings = AgentSettings()
storage = Storage()


async def build_prober_parameters(request):
    """Build prober parameters depending on the request."""
    request_parameters = request["parameters"]
    del request["parameters"]
    return {**request, **request_parameters}


async def measuremement(redis, request):
    """Conduct a measurement."""
    measurement_uuid = request["measurement_uuid"]
    agent_uuid = redis.uuid

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"

    parameters = await build_prober_parameters(request)
    if agent_uuid != parameters["agent_uuid"]:
        logger.error(f"{logger_prefix} Invalid agent UUID in measurement parameters")

    measurement_results_path = settings.AGENT_RESULTS_DIR_PATH / measurement_uuid
    logger.info(f"{logger_prefix} Create local measurement directory")
    try:
        await aios.mkdir(str(measurement_results_path))
    except FileExistsError:
        logger.warning(f"{logger_prefix} Local measurement directory already exits")

    round_number = parameters["round"]
    username = parameters["username"]

    result_filename = f"{agent_uuid}_results_{round_number}.pcap"
    result_filepath = str(measurement_results_path / result_filename)
    starttime_filename = f"{agent_uuid}_starttime_{round_number}.log"
    starttime_filepath = str(measurement_results_path / starttime_filename)

    if round_number == 1:
        if parameters["full"] and parameters["targets_file_key"] is None:
            logger.info(f"{logger_prefix} Full snapshot required")
            target_filepath = None
            target_type = None
        else:
            logger.info(f"{logger_prefix} Target file based snapshot required")
            logger.info(f"{logger_prefix} Download target file locally")
            target_filename = parameters["targets_file_key"]
            target_filepath = str(settings.AGENT_TARGETS_DIR_PATH / target_filename)
            target = await storage.get_file(
                settings.AWS_S3_TARGETS_BUCKET_PREFIX + username, target_filename
            )
            target_type = target.get("metadata", {}).get("type", "targets-list")
            await storage.download_file(
                settings.AWS_S3_TARGETS_BUCKET_PREFIX + username,
                target_filename,
                target_filepath,
            )
        csv_filepath = None
    else:
        logger.info(f"{logger_prefix} Download CSV probe file locally")
        target_filepath = None
        target_type = None
        csv_filename = request["probes"]
        csv_filepath = str(settings.AGENT_TARGETS_DIR_PATH / csv_filename)
        await storage.download_file(measurement_uuid, csv_filename, csv_filepath)

    logger.info(f"{logger_prefix} Tool : {parameters['measurement_tool']}")
    logger.info(f"{logger_prefix} Username : {parameters['username']}")
    logger.info(f"{logger_prefix} Round : {parameters['round']}")
    logger.info(f"{logger_prefix} Minimum TTL : {parameters['min_ttl']}")
    logger.info(f"{logger_prefix} Maximum TTL : {parameters['max_ttl']}")
    logger.info(f"{logger_prefix} Probing Rate : {parameters['probing_rate']}")
    is_not_canceled = await probe(
        parameters,
        result_filepath,
        starttime_filepath,
        target_filepath=target_filepath,
        target_type=target_type,
        csv_filepath=csv_filepath,
        stopper=stopper(
            logger, redis, measurement_uuid, logger_prefix=logger_prefix + " "
        ),
        logger_prefix=logger_prefix + " ",
    )

    if is_not_canceled:
        logger.info(
            f"{logger_prefix} Upload result file & start time log file into AWS S3"
        )
        await storage.upload_file(measurement_uuid, result_filename, result_filepath)
        await storage.upload_file(
            measurement_uuid, starttime_filename, starttime_filepath
        )

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
