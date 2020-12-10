"""Measurement interface."""

import aiofiles
import aiofiles.os

from iris.agent import logger
from iris.agent.prober import probe, stopper
from iris.agent.settings import AgentSettings
from iris.commons.storage import Storage

from diamond_miner_core import SequentialFlowMapper
from diamond_miner_core.rounds import exhaustive_round, targets_round, probe_to_csv


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
        await aiofiles.os.mkdir(str(measurement_results_path))
    except FileExistsError:
        logger.warning(f"{logger_prefix} Local measurement directory already exits")

    result_filename = f"{agent_uuid}_results_{parameters['round']}.pcap"
    results_filepath = str(measurement_results_path / result_filename)

    stdin = None
    prefix_incl_filepath = None
    targets_filepath = None
    probes_filepath = None

    if parameters["round"] == 1:
        # Round = 1
        if parameters["full"] and parameters["targets_file_key"] is None:
            # Exhaustive snapshot
            logger.info(f"{logger_prefix} Full snapshot required")
            stdin = (
                probe_to_csv(*x)
                async for x in exhaustive_round(
                    SequentialFlowMapper(),
                    dst_port=parameters["destination_port"],
                    n_flows=settings.AGENT_IPS_PER_SUBNET,
                )
            )
        else:
            # Targets-list or prefixes-list
            logger.info(f"{logger_prefix} Download targets/prefixes file locally")
            targets_filename = parameters["targets_file_key"]
            targets_filepath = str(settings.AGENT_TARGETS_DIR_PATH / targets_filename)
            targets_info = await storage.get_file(
                settings.AWS_S3_TARGETS_BUCKET_PREFIX + parameters["username"],
                targets_filename,
            )
            targets_type = targets_info.get("metadata", {}).get("type", "targets-list")
            await storage.download_file(
                settings.AWS_S3_TARGETS_BUCKET_PREFIX + parameters["username"],
                targets_filename,
                targets_filepath,
            )
            if targets_type == "targets-list":
                # Targets-list file
                async with aiofiles.open(targets_filepath) as fd:
                    targets_list = await fd.readlines()
                stdin = (
                    probe_to_csv(*x)
                    async for x in targets_round(
                        targets_list, dst_port=parameters["destination_port"]
                    )
                )
            elif targets_type == "prefixes-list":
                # Prefixes-list file
                stdin = (
                    probe_to_csv(*x)
                    async for x in exhaustive_round(
                        SequentialFlowMapper(),
                        dst_port=parameters["destination_port"],
                        n_flows=settings.AGENT_IPS_PER_SUBNET,
                    )
                )
                prefix_incl_filepath = targets_filepath
            else:
                logger.error("Unknown target file type")
                return
    else:
        # Round > 1
        logger.info(f"{logger_prefix} Download CSV probe file locally")
        probes_filename = request["probes"]
        probes_filepath = str(settings.AGENT_TARGETS_DIR_PATH / probes_filename)
        await storage.download_file(measurement_uuid, probes_filename, probes_filepath)

    logger.info(f"{logger_prefix} Tool : {parameters['measurement_tool']}")
    logger.info(f"{logger_prefix} Username : {parameters['username']}")
    logger.info(f"{logger_prefix} Round : {parameters['round']}")
    logger.info(f"{logger_prefix} Minimum TTL : {parameters['min_ttl']}")
    logger.info(f"{logger_prefix} Maximum TTL : {parameters['max_ttl']}")
    logger.info(f"{logger_prefix} Probing Rate : {parameters['probing_rate']}")
    is_not_canceled = await probe(
        parameters,
        results_filepath,
        stdin=stdin,
        prefix_incl_filepath=prefix_incl_filepath,
        probes_filepath=probes_filepath,
        stopper=stopper(
            logger, redis, measurement_uuid, logger_prefix=logger_prefix + " "
        ),
        logger_prefix=logger_prefix + " ",
    )

    if is_not_canceled:
        logger.info(f"{logger_prefix} Upload results file into AWS S3")
        await storage.upload_file(measurement_uuid, result_filename, results_filepath)

    if not settings.AGENT_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local result file")
        await aiofiles.os.remove(results_filepath)

    if not settings.AGENT_DEBUG_MODE:
        logger.info(f"{logger_prefix} Removing local measurement directory")
        try:
            await aiofiles.os.rmdir(str(measurement_results_path))
        except OSError:
            logger.error(
                f"{logger_prefix} Impossible to remove local measurement directory"
            )

    if targets_filepath is not None:
        logger.info(f"{logger_prefix} Remove local target file")
        await aiofiles.os.remove(targets_filepath)

    if probes_filepath is not None:
        if not settings.AGENT_DEBUG_MODE:
            logger.info(f"{logger_prefix} Remove local CSV probes file")
            await aiofiles.os.remove(probes_filepath)

        logger.info(f"{logger_prefix} Remove CSV probe file from AWS S3")
        response = await storage.delete_file_no_check(measurement_uuid, probes_filename)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
            logger.error(f"Impossible to remove result file `{probes_filename}`")
