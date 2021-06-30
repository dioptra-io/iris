"""Measurement interface."""

from ipaddress import ip_address, ip_network
from multiprocessing import Manager, Process

import aiofiles
import aiofiles.os
import radix
from diamond_miner import mappers
from diamond_miner.defaults import DEFAULT_PREFIX_SIZE_V4, DEFAULT_PREFIX_SIZE_V6

from iris.agent.prober import probe, watcher
from iris.commons.dataclasses import ParametersDataclass
from iris.commons.round import Round


def cast_addr(addr: str) -> str:
    base_addr = ip_address(addr)
    base_addr_mapped = base_addr.ipv4_mapped
    if base_addr_mapped:
        return str(base_addr_mapped)
    return str(base_addr)


def addr_to_network(addr: str, prefix_len_v4: int = 24, prefix_len_v6: int = 64) -> str:
    base_addr = ip_address(addr)
    if base_addr.version == 4:
        return str(ip_network(addr + f"/{prefix_len_v4}"))
    return str(ip_network(addr + f"/{prefix_len_v6}"))


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

        prefixes_from_target_file = radix.Radix()
        async with aiofiles.open(target_filepath) as fd:
            async for target in fd:
                target_line = target.split(",")

                min_ttl = max(
                    settings.AGENT_MIN_TTL, int(target_line[2]), round.min_ttl
                )
                max_ttl = min(int(target_line[3]), round.max_ttl)

                node = prefixes_from_target_file.add(target_line[0])
                if node.data.get("todo"):
                    node.data["todo"].append(
                        (target_line[1], range(min_ttl, max_ttl + 1))
                    )
                else:
                    node.data["todo"] = [(target_line[1], range(min_ttl, max_ttl + 1))]

        prefixes_addr_to_probe = None
        if prefix_filepath is not None:
            # There is a list of prefixes to probe
            # So we use these prefixes along with the TTL information
            # from the prefix list
            async with aiofiles.open(prefix_filepath) as fd:
                prefixes_addr_to_probe = await fd.readlines()

            prefixes_addr_to_probe = [
                cast_addr(p.strip()) for p in prefixes_addr_to_probe
            ]

            prefixes = []
            for prefix_addr in prefixes_addr_to_probe:
                node = prefixes_from_target_file.search_best(prefix_addr)
                for todo in node.data["todo"]:
                    prefixes.append((addr_to_network(prefix_addr), todo[0], todo[1]))
        else:
            # There is no prefix list to probe so we directly take the target list
            prefixes = []
            for node in prefixes_from_target_file:
                for todo in node.data["todo"]:
                    prefixes.append((node.prefix, todo[0], todo[1]))

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

    with Manager() as manager:
        prober_statistics = manager.dict()
        sniffer_statistics = manager.dict()

        prober_process = Process(
            target=probe,
            args=(
                settings,
                results_filepath,
                round.number,
                parameters.probing_rate,
                prober_statistics,
                sniffer_statistics,
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

        prober_statistics = dict(prober_statistics)
        sniffer_statistics = dict(sniffer_statistics)

    statistics = {**prober_statistics, **sniffer_statistics}
    if redis:
        logger.info("Upload probing statistics in Redis")
        await redis.set_measurement_stats(measurement_uuid, agent_uuid, statistics)

    if is_not_canceled:
        logger.info(f"{logger_prefix} Upload results file into AWS S3")
        await storage.upload_file(measurement_uuid, results_filename, results_filepath)

    if not settings.AGENT_DEBUG_MODE:
        logger.info(f"{logger_prefix} Remove local results file")
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
            logger.error(f"Impossible to remove results file `{probes_filename}`")

    return results_filename, statistics
