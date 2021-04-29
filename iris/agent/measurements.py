"""Measurement interface."""

import multiprocessing

import aiofiles
import aiofiles.os
from diamond_miner import mappers
from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4, DEFAULT_PREFIX_SIZE_V6

from iris.agent.prober import probe, watcher
from iris.commons.dataclasses import ParametersDataclass


def build_probe_generator_parameters(target_list, parameters):
    flow_mapper_cls = getattr(mappers, parameters.tool_parameters["flow_mapper"])
    flow_mapper_kwargs = parameters.tool_parameters["flow_mapper_kwargs"] or {}

    if parameters.tool in ["diamond-miner", "yarrp"]:
        flow_mapper_v4 = flow_mapper_cls(
            **{"prefix_size": DEFAULT_PREFIX_SIZE_V4, **flow_mapper_kwargs}
        )
        flow_mapper_v6 = flow_mapper_cls(
            **{"prefix_size": DEFAULT_PREFIX_SIZE_V6, **flow_mapper_kwargs}
        )

        prefixes = []
        for target in target_list:
            target_line = target.split(",")
            prefixes.append(
                (
                    target_line[0],
                    target_line[1],
                    range(int(target_line[2]), int(target_line[3]) + 1),
                )
            )

        return {
            "prefixes": prefixes,
            "prefix_len_v4": 24,
            "prefix_len_v6": 64,
            "flow_ids": range(parameters.tool_parameters["n_flow_ids"]),
            "probe_dst_port": parameters.tool_parameters["destination_port"],
            "mapper_v4": flow_mapper_v4,
            "mapper_v6": flow_mapper_v6,
        }
    elif parameters.tool == "ping":
        flow_mapper_v4 = flow_mapper_cls(**{"prefix_size": 1, **flow_mapper_kwargs})
        flow_mapper_v6 = flow_mapper_cls(**{"prefix_size": 1, **flow_mapper_kwargs})

        # Only take the max TTL in the TTL range
        prefixes = []
        for target in target_list:
            target_line = target.split(",")
            prefixes.append(
                (
                    target_line[0],
                    target_line[1],
                    [int(target_line[3])],
                )
            )

        return {
            "prefixes": prefixes,
            "prefix_len_v4": 32,
            "prefix_len_v6": 128,
            "flow_ids": range(parameters.tool_parameters["n_flow_ids"]),
            "probe_dst_port": parameters.tool_parameters["destination_port"],
            "mapper_v4": flow_mapper_v4,
            "mapper_v6": flow_mapper_v6,
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

    gen_parameters = None

    target_filepath = None
    probes_filepath = None

    if request["round"] == 1:
        # Round = 1
        logger.info(f"{logger_prefix} Download targets/prefixes file locally")
        target_filename = parameters.target_file
        target_filepath = str(settings.AGENT_TARGETS_DIR_PATH / target_filename)
        await storage.download_file(
            settings.AWS_S3_TARGETS_BUCKET_PREFIX + request["username"],
            target_filename,
            target_filepath,
        )
        async with aiofiles.open(target_filepath) as fd:
            target_list = await fd.readlines()

        gen_parameters = build_probe_generator_parameters(target_list, parameters)

    else:
        # Round > 1
        logger.info(f"{logger_prefix} Download CSV probe file locally")
        probes_filename = request["probes"]
        probes_filepath = str(settings.AGENT_TARGETS_DIR_PATH / probes_filename)
        await storage.download_file(measurement_uuid, probes_filename, probes_filepath)

    logger.info(f"{logger_prefix} Username : {request['username']}")
    logger.info(f"{logger_prefix} Target File: {parameters.target_file}")
    logger.info(f"{logger_prefix} Tool : {parameters.tool}")
    logger.info(f"{logger_prefix} Tool Parameters : {parameters.tool_parameters}")
    logger.info(f"{logger_prefix} Max Probing Rate : {parameters.probing_rate}")

    prober_process = multiprocessing.Process(
        target=probe,
        args=(
            settings,
            results_filepath,
            request["round"],
            parameters.probing_rate,
            gen_parameters,
            probes_filepath,
        ),
    )

    prober_process.start()
    is_not_canceled = await watcher(
        prober_process,
        settings,
        measurement_uuid,
        logger,
        logger_prefix=logger_prefix,
        redis=redis,
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

    if target_filepath is not None:
        logger.info(f"{logger_prefix} Remove local target file")
        await aiofiles.os.remove(target_filepath)

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
