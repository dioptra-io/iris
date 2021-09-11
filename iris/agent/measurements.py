"""Measurement interface."""
from datetime import datetime
from logging import Logger
from multiprocessing import Manager, Process
from typing import Dict, Iterable, List, Optional, Tuple

import aiofiles
import aiofiles.os
from diamond_miner import mappers
from pytricia import PyTricia

from iris.agent.prober import probe, watcher
from iris.agent.settings import AgentSettings
from iris.commons.redis import AgentRedis
from iris.commons.schemas.private import MeasurementRoundRequest
from iris.commons.schemas.public import ProbingStatistics, Round, Tool, ToolParameters
from iris.commons.storage import Storage, targets_key


def build_probe_generator_parameters(
    agent_min_ttl: int,
    round_: Round,
    tool: Tool,
    tool_parameters: ToolParameters,
    target_list: Iterable[str],
    prefix_list: Optional[Iterable[str]],
) -> Dict:
    """
    Target list format: `prefix,protocol,min_ttl,max_ttl`
    Prefix list format: `prefix`
    For both lists, `prefix` can be:
        * a network: 8.8.8.0/24, 2001:4860:4860::/64
        * an address: 8.8.8.8, 2001:4860:4860::8888
    Addresses are interpreted as /32 or /128 networks.
    """
    # 1. Instantiate the flow mappers
    flow_mapper_cls = getattr(mappers, tool_parameters.flow_mapper)
    flow_mapper_kwargs = tool_parameters.flow_mapper_kwargs or {}
    flow_mapper_v4 = flow_mapper_cls(
        **{"prefix_size": tool_parameters.prefix_size_v4, **flow_mapper_kwargs}
    )
    flow_mapper_v6 = flow_mapper_cls(
        **{"prefix_size": tool_parameters.prefix_size_v6, **flow_mapper_kwargs}
    )

    prefixes: List[Tuple[str, str, Iterable[int]]] = []
    if tool in [Tool.DiamondMiner, Tool.Yarrp]:
        # 2. Build a radix tree that maps prefix -> [(min_ttl...max_ttl), ...]
        targets = PyTricia(128)
        for line in target_list:
            prefix, protocol, min_ttl, max_ttl = line.split(",")
            ttls = range(
                # Ensure that the prefix minimum TTL is superior to:
                # - the agent minimum TTL
                # - the round minimum TTL
                max(agent_min_ttl, int(min_ttl), round_.min_ttl),
                # Ensure that the prefix maximum TTL is inferior to the round maximum TTL
                min(int(max_ttl), round_.max_ttl) + 1,
            )
            if todo := targets.get(prefix):
                todo.append((protocol, ttls))
            else:
                targets[prefix] = [(protocol, ttls)]

        # 3. If a specific list of prefixes to probe is specified, generate a new list of prefixes
        # that includes the TTL ranges previously loaded.
        if prefix_list is not None:
            for line in prefix_list:
                prefix = line.strip()
                todo = targets[prefix]
                for protocol, ttls in todo:
                    prefixes.append((prefix, protocol, ttls))
        else:
            # There is no prefix list to probe so we directly take the target list
            for prefix in targets:
                for protocol, ttls in targets[prefix]:
                    prefixes.append((prefix, protocol, ttls))

    elif tool == Tool.Ping:
        # Only take the max TTL in the TTL range
        for line in target_list:
            prefix, protocol, min_ttl, max_ttl = line.split(",")
            prefixes.append((prefix, protocol, (int(max_ttl),)))

    return {
        "prefixes": prefixes,
        "prefix_len_v4": tool_parameters.prefix_len_v4,
        "prefix_len_v6": tool_parameters.prefix_len_v6,
        "flow_ids": range(tool_parameters.n_flow_ids),
        "probe_dst_port": tool_parameters.destination_port,
        "mapper_v4": flow_mapper_v4,
        "mapper_v6": flow_mapper_v6,
    }


async def measurement(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    logger: Logger,
    redis: AgentRedis,
    storage: Storage,
) -> Tuple[str, ProbingStatistics]:
    """Conduct a measurement."""
    measurement_request = request.measurement
    agent = measurement_request.agent(settings.AGENT_UUID)
    logger_prefix = f"{measurement_request.uuid} :: {agent.uuid} ::"

    measurement_results_path = settings.AGENT_RESULTS_DIR_PATH / str(
        measurement_request.uuid
    )
    logger.info(f"{logger_prefix} Create local measurement directory")
    try:
        await aiofiles.os.mkdir(str(measurement_results_path))
    except FileExistsError:
        logger.warning(f"{logger_prefix} Local measurement directory already exits")

    results_filename = f"{agent.uuid}_results_{request.round.encode()}.csv.zst"
    results_filepath = str(measurement_results_path / results_filename)

    gen_parameters = None
    target_filepath = None
    probes_filepath = None
    is_custom_probes_file = agent.target_file.endswith(".probes")

    if request.round.number == 1 and not is_custom_probes_file:
        assert agent.uuid
        # Round = 1
        # No custom probe file uploaded in advance
        logger.info(f"{logger_prefix} Download target file locally")
        target_filepath = await storage.download_file_to(
            storage.archive_bucket(measurement_request.username),
            targets_key(measurement_request.uuid, agent.uuid),
            settings.AGENT_TARGETS_DIR_PATH,
        )

        prefix_filename = request.probes  # we use the same key as probe file
        prefix_filepath = None
        if prefix_filename:
            logger.info(f"{logger_prefix} Download CSV prefix file locally")
            prefix_filepath = await storage.download_file_to(
                storage.measurement_bucket(measurement_request.uuid),
                prefix_filename,
                settings.AGENT_TARGETS_DIR_PATH,
            )

        logger.info(f"{logger_prefix} Build probe generator parameters")
        gen_parameters = build_probe_generator_parameters(
            settings.AGENT_MIN_TTL,
            request.round,
            measurement_request.tool,
            agent.tool_parameters,
            open(target_filepath),
            open(prefix_filepath) if prefix_filepath else None,
        )

    elif request.round.number == 1 and is_custom_probes_file:
        # Round = 1
        # Custom probe file uploaded in advance
        logger.info(f"{logger_prefix} Download custom CSV probe file locally")
        probes_filename = agent.target_file
        probes_filepath = await storage.download_file_to(
            storage.targets_bucket(measurement_request.username),
            agent.target_file,
            settings.AGENT_TARGETS_DIR_PATH,
        )

    elif request.probes:
        # Round > 1
        logger.info(f"{logger_prefix} Download CSV probe file locally")
        probes_filename = request.probes
        probes_filepath = await storage.download_file_to(
            storage.measurement_bucket(measurement_request.uuid),
            request.probes,
            settings.AGENT_TARGETS_DIR_PATH,
        )

    logger.info(f"{logger_prefix} Username : {measurement_request.username}")
    logger.info(f"{logger_prefix} Target File: {agent.target_file}")
    logger.info(f"{logger_prefix} {request.round}")
    logger.info(f"{logger_prefix} Tool : {measurement_request.tool}")
    logger.info(f"{logger_prefix} Tool Parameters : {agent.tool_parameters}")
    logger.info(f"{logger_prefix} Max Probing Rate : {agent.probing_rate}")

    probing_start_time = datetime.now()
    with Manager() as manager:
        prober_statistics = manager.dict()  # type: ignore

        prober_process = Process(
            target=probe,
            args=(
                settings,
                str(results_filepath),
                request.round.number,
                agent.probing_rate,
                prober_statistics,
                gen_parameters,
                str(probes_filepath),
            ),
        )

        prober_process.start()
        is_not_canceled = await watcher(
            prober_process,
            settings,
            measurement_request.uuid,
            redis,
            logger,
            logger_prefix=logger_prefix,
        )

        prober_statistics = dict(prober_statistics)

    logger.info("Upload probing statistics in Redis")
    statistics = ProbingStatistics(
        round=request.round,
        start_time=probing_start_time,
        end_time=datetime.now(),
        **prober_statistics,
    )
    await redis.set_measurement_stats(measurement_request.uuid, agent.uuid, statistics)

    if is_not_canceled:
        logger.info(f"{logger_prefix} Upload results file into AWS S3")
        await storage.upload_file(
            storage.measurement_bucket(measurement_request.uuid),
            results_filename,
            results_filepath,
        )

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
            str(measurement_request.uuid), probes_filename
        )
        if not is_deleted:
            logger.error(f"Impossible to remove results file `{probes_filename}`")

    return results_filename, statistics
