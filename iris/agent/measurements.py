"""Measurement interface."""

import multiprocessing
from ipaddress import ip_address, ip_network

import aiofiles
import aiofiles.os
from diamond_miner import mappers
from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4, DEFAULT_PREFIX_SIZE_V6

from iris.agent.prober import probe, watcher
from iris.commons.dataclasses import ParametersDataclass
from iris.commons.round import Round


def cast_prefix(prefix: str):
    base_prefix = ip_address(prefix)
    base_prefix_mapped = base_prefix.ipv4_mapped
    if base_prefix_mapped:
        return ip_network(str(base_prefix_mapped) + "/24")
    return ip_network(prefix + "/64")


async def build_probe_generator_parameters(
    settings, target_filepath, prefix_filepath, round, parameters
):
    flow_mapper_cls = getattr(mappers, parameters.tool_parameters["flow_mapper"])
    flow_mapper_kwargs = parameters.tool_parameters["flow_mapper_kwargs"] or {}

    if parameters.tool in ["diamond-miner", "yarrp"]:
        flow_mapper_v4 = flow_mapper_cls(
            **{"prefix_size": DEFAULT_PREFIX_SIZE_V4, **flow_mapper_kwargs}
        )
        flow_mapper_v6 = flow_mapper_cls(
            **{"prefix_size": DEFAULT_PREFIX_SIZE_V6, **flow_mapper_kwargs}
        )

        prefixes_from_target_file = []
        async with aiofiles.open(target_filepath) as fd:
            async for target in fd:
                target_line = target.split(",")

                min_ttl = max(
                    settings.AGENT_MIN_TTL, int(target_line[2]), round.min_ttl
                )
                max_ttl = min(int(target_line[3]), round.max_ttl)

                prefixes_from_target_file.append(
                    [
                        ip_network(target_line[0]),
                        target_line[1],
                        range(min_ttl, max_ttl + 1),
                    ]
                )

        prefixes_to_probe = None
        if prefix_filepath is not None:
            # There is a list of prefixes to probe
            # So we use these prefixes along with the TTL information
            # from the prefix list
            async with aiofiles.open(prefix_filepath) as fd:
                prefixes_to_probe = await fd.readlines()

            prefixes_to_probe = [cast_prefix(p.strip()) for p in prefixes_to_probe]

            prefixes = []
            for prefix in prefixes_to_probe:
                for prefix_target in prefixes_from_target_file:
                    if prefix.overlaps(prefix_target[0]):
                        prefixes.append(tuple([str(prefix)] + prefix_target[1:]))
        else:
            # There is no prefix list to probe so we directly take the target list
            prefixes = []
            for prefix in prefixes_from_target_file:
                prefixes.append(tuple([str(prefix[0])] + prefix[1:]))

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
        async with aiofiles.open(target_filepath) as fd:
            async for target in fd:
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
    round = Round.decode(request["round"])

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

    results_filename = f"{agent_uuid}_results_{round.encode()}.csv.zst"
    results_filepath = str(measurement_results_path / results_filename)

    gen_parameters = None
    target_filepath = None

    probes_filepath = None

    is_custom_probes_file = parameters.target_file.endswith(".probes")

    if round.number == 1 and not is_custom_probes_file:
        # Round = 1
        # No custom probe file uploaded in advance
        logger.info(f"{logger_prefix} Download target file locally")
        target_filename = f"targets__{measurement_uuid}__{agent_uuid}.csv"
        target_filepath = str(settings.AGENT_TARGETS_DIR_PATH / target_filename)
        await storage.download_file(
            settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + request["username"],
            target_filename,
            target_filepath,
        )

        prefix_filename = request["probes"]  # we use the same key as probe file
        prefix_filepath = None
        if prefix_filename:
            logger.info(f"{logger_prefix} Download CSV prefix file locally")
            prefix_filepath = str(settings.AGENT_TARGETS_DIR_PATH / prefix_filename)
            await storage.download_file(
                measurement_uuid, prefix_filename, prefix_filepath
            )

        gen_parameters = await build_probe_generator_parameters(
            settings, target_filepath, prefix_filepath, round, parameters
        )

    elif round.number == 1 and is_custom_probes_file:
        # Round = 1
        # Custom probe file uploaded in advance
        logger.info(f"{logger_prefix} Download custom CSV probe file locally")
        probes_filename = parameters.target_file
        probes_filepath = str(settings.AGENT_TARGETS_DIR_PATH / probes_filename)
        await storage.download_file(
            settings.AWS_S3_TARGETS_BUCKET_PREFIX + request["username"],
            probes_filename,
            probes_filepath,
        )

    else:
        # Round > 1
        logger.info(f"{logger_prefix} Download CSV probe file locally")
        probes_filename = request["probes"]
        probes_filepath = str(settings.AGENT_TARGETS_DIR_PATH / probes_filename)
        await storage.download_file(measurement_uuid, probes_filename, probes_filepath)

    logger.info(f"{logger_prefix} Username : {request['username']}")
    logger.info(f"{logger_prefix} Target File: {parameters.target_file}")
    logger.info(f"{logger_prefix} {round}")
    logger.info(f"{logger_prefix} Tool : {parameters.tool}")
    logger.info(f"{logger_prefix} Tool Parameters : {parameters.tool_parameters}")
    logger.info(f"{logger_prefix} Max Probing Rate : {parameters.probing_rate}")

    prober_process = multiprocessing.Process(
        target=probe,
        args=(
            settings,
            results_filepath,
            round.number,
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
