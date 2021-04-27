"""Measurement interface."""

import aiofiles
import aiofiles.os
from diamond_miner import mappers
from diamond_miner.generator import probe_generator_by_flow
from diamond_miner.utilities import format_probe

from iris.agent.prober import probe, stopper
from iris.commons.dataclasses import ParametersDataclass


def build_probe_generator_parameters(targets_list, parameters):

    if parameters.tool in ["diamond-miner", "yarrp"]:
        flow_mapper_cls = getattr(mappers, parameters.tool_parameters["flow_mapper"])
        flow_mapper_kwargs = parameters.tool_parameters["flow_mapper_kwargs"] or {}
        flow_mapper = flow_mapper_cls(**flow_mapper_kwargs)

        return {
            "prefixes": [
                (
                    prefix,
                    parameters.tool_parameters["protocol"],
                    range(
                        parameters.tool_parameters["min_ttl"],
                        parameters.tool_parameters["max_ttl"] + 1,
                    ),
                )
                for prefix in targets_list
            ],
            "prefix_len_v4": 24,
            "prefix_len_v6": 64,
            "flow_ids": range(parameters.tool_parameters["n_flow_ids"]),
            "probe_dst_port": parameters.tool_parameters["destination_port"],
            "mapper_v4": flow_mapper,
            "mapper_v6": flow_mapper,
        }
    elif parameters.tool == "ping":
        return {
            "prefixes": [
                (
                    prefix,
                    parameters.tool_parameters["protocol"],
                    [parameters.tool_parameters["max_ttl"]],
                )
                for prefix in targets_list
            ],
            "prefix_len_v4": 32,
            "prefix_len_v6": 128,
            "flow_ids": range(parameters.tool_parameters["n_flow_ids"]),
            "probe_dst_port": parameters.tool_parameters["destination_port"],
        }
    else:
        raise ValueError("Invalid tool name")


async def measurement(settings, request, storage, logger, redis=None):
    """Conduct a measurement."""
    measurement_uuid = request["measurement_uuid"]
    agent_uuid = settings.AGENT_UUID

    logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"

    parameters = ParametersDataclass.from_request(request)
    if agent_uuid != parameters.agent_uuid:
        logger.error(f"{logger_prefix} Invalid agent UUID in measurement parameters")

    measurement_results_path = settings.AGENT_RESULTS_DIR_PATH / measurement_uuid
    logger.info(f"{logger_prefix} Create local measurement directory")
    try:
        await aiofiles.os.mkdir(str(measurement_results_path))
    except FileExistsError:
        logger.warning(f"{logger_prefix} Local measurement directory already exits")

    results_filename = f"{agent_uuid}_results_{request['round']}.csv"
    results_filepath = str(measurement_results_path / results_filename)

    stdin = None
    targets_filepath = None
    probes_filepath = None

    if request["round"] == 1:
        # Round = 1
        logger.info(f"{logger_prefix} Download targets/prefixes file locally")
        targets_filename = parameters.targets_file
        targets_filepath = str(settings.AGENT_TARGETS_DIR_PATH / targets_filename)
        await storage.download_file(
            settings.AWS_S3_TARGETS_BUCKET_PREFIX + request["username"],
            targets_filename,
            targets_filepath,
        )
        async with aiofiles.open(targets_filepath) as fd:
            targets_list = await fd.readlines()

        gen = probe_generator_by_flow(
            **build_probe_generator_parameters(targets_list, parameters)
        )
        stdin = (format_probe(*x) for x in gen)
    else:
        # Round > 1
        logger.info(f"{logger_prefix} Download CSV probe file locally")
        probes_filename = request["probes"]
        probes_filepath = str(settings.AGENT_TARGETS_DIR_PATH / probes_filename)
        await storage.download_file(measurement_uuid, probes_filename, probes_filepath)

    logger.info(f"{logger_prefix} Username : {request['username']}")
    logger.info(f"{logger_prefix} Target File: {parameters.targets_file}")
    logger.info(f"{logger_prefix} Tool : {parameters.tool}")
    logger.info(f"{logger_prefix} Tool Parameters : {parameters.tool_parameters}")
    logger.info(f"{logger_prefix} Max Probing Rate : {parameters.probing_rate}")

    if redis:
        stopper_func = stopper(
            settings, redis, measurement_uuid, logger, logger_prefix=logger_prefix + " "
        )
    else:
        stopper_func = None

    is_not_canceled = await probe(
        settings,
        parameters,
        request["round"],
        results_filepath,
        logger,
        stdin=stdin,
        probes_filepath=probes_filepath,
        stopper=stopper_func,
        logger_prefix=logger_prefix + " ",
    )

    if is_not_canceled:
        logger.info(f"{logger_prefix} Upload results file into AWS S3")
        await storage.upload_file(measurement_uuid, results_filename, results_filepath)

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
        is_deleted = await storage.delete_file_no_check(
            measurement_uuid, probes_filename
        )
        if not is_deleted:
            logger.error(f"Impossible to remove result file `{probes_filename}`")

    return results_filename
