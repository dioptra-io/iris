import subprocess
from collections import defaultdict
from logging import Logger
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from diamond_miner import mappers
from diamond_miner.generators import probe_generator_parallel
from diamond_miner.insert import insert_mda_probe_counts_parallel, insert_probe_counts
from diamond_miner.queries import GetSlidingPrefixes
from diamond_miner.typing import FlowMapper
from zstandard import ZstdDecompressor

from iris.commons.clickhouse import ClickHouse
from iris.commons.models.diamond_miner import Tool, ToolParameters
from iris.commons.models.round import Round
from iris.commons.results import InsertResults
from iris.worker.tree import load_targets


async def default_inner_pipeline(
    clickhouse: ClickHouse,
    logger: Logger,
    # NOTE: Ideally we would not need to pass the UUIDs here,
    # but rather directly a database/table.
    measurement_uuid: str,
    agent_uuid: str,
    agent_min_ttl: int,
    measurement_tags: List[str],
    # NOTE: Ideally the sliding window parameters would be tool parameters.
    # Iris shouldn't need to know about this feature.
    sliding_window_stopping_condition: int,
    tool: Tool,
    tool_parameters: ToolParameters,
    results_filepath: Optional[Path],
    targets_filepath: Path,
    probes_filepath: Path,
    previous_round: Optional[Round],
    next_round: Round,
) -> int:
    """
    Given a targets file and an optional results file, write the probes for the next round.
    This is a generic implementation for the tools based on the diamond-miner library:
    diamond-miner, yarrp and ping.

    :returns: The number of probes written.
    """

    def log(s):
        logger.info(f"{measurement_uuid} :: {agent_uuid} :: {s}")

    database_url = clickhouse.settings.CLICKHOUSE_URL
    measurement_id = f"{measurement_uuid}__{agent_uuid}"

    flow_mapper_v4, flow_mapper_v6 = instantiate_flow_mappers(
        tool_parameters.flow_mapper,
        tool_parameters.flow_mapper_kwargs or {},
        tool_parameters.prefix_size_v4,
        tool_parameters.prefix_size_v6,
    )

    if results_filepath:
        insert_results = InsertResults(
            clickhouse,
            measurement_uuid,
            agent_uuid,
            tool_parameters.prefix_len_v4,
            tool_parameters.prefix_len_v6,
        )
        log("Create results tables")
        await insert_results.create_table()

        # NOTE: For now this feature is activated only for default inner pipeline.
        # Not for the probes inner pipeline.
        if "public" in measurement_tags:  # TODO parametrize public tag name
            log("Grant public access to results tables (if public user is set)")
            await insert_results.grant_public_access()

        log("Insert results file")
        await insert_results.insert_csv(results_filepath)
        log("Insert prefixes")
        await insert_results.insert_prefixes()
        log("Insert links")
        await insert_results.insert_links()

    probe_ttl_geq = 0
    probe_ttl_leq = 255

    # Compute the sub-rounds of round 1.
    if next_round.number == 1:
        probe_ttl_geq = max(agent_min_ttl, next_round.min_ttl)
        probe_ttl_leq = next_round.max_ttl
        log(f"Next round window: TTL {probe_ttl_geq} to {probe_ttl_leq} (incl.)")

        log("Load targets")
        with targets_filepath.open() as f:
            targets = load_targets(
                f,
                clamp_ttl_min=probe_ttl_geq,
                clamp_ttl_max=probe_ttl_leq,
            )

        log("Compute the prefixes to probe")
        prefixes = []

        if previous_round is None:
            log("Enumerate initial prefixes")
            for prefix in targets:
                for protocol, ttls, n_initial_flows in targets[prefix]:
                    if tool == Tool.Ping:
                        # In the case of ping, only take the max TTL in the TTL range.
                        ttls = (ttls[-1],)
                    prefixes.append((prefix, protocol, ttls, n_initial_flows))
        else:
            log("Enumerate sliding prefixes")
            query = GetSlidingPrefixes(
                window_max_ttl=previous_round.max_ttl,
                stopping_condition=sliding_window_stopping_condition,
            )
            for _, _, addr_v6 in query.execute_iter(database_url, measurement_id):
                if addr_v4 := addr_v6.ipv4_mapped:
                    prefix = f"{addr_v4}/{tool_parameters.prefix_len_v4}"
                else:
                    prefix = f"{addr_v6}/{tool_parameters.prefix_len_v6}"
                for protocol, ttls, n_initial_flows in targets[prefix]:
                    if tool == Tool.Ping:
                        # In the case of ping, only take the max TTL in the TTL range.
                        ttls = (ttls[-1],)
                    prefixes.append((prefix, protocol, ttls, n_initial_flows))

        log("Insert probe counts")
        insert_probe_counts(
            url=database_url,
            measurement_id=measurement_id,
            round_=next_round.number,
            prefixes=prefixes,
            prefix_len_v4=tool_parameters.prefix_len_v4,
            prefix_len_v6=tool_parameters.prefix_len_v6,
        )

        del prefixes, targets

    # Compute MDA probes for round > 1
    else:
        assert previous_round, "round > 1 must have a previous round"
        log("Insert MDA probe counts")
        insert_mda_probe_counts_parallel(
            url=database_url,
            measurement_id=measurement_id,
            previous_round=previous_round.number,
            target_epsilon=tool_parameters.failure_rate,
            adaptive_eps=True,
        )

    log("Generate probes file")
    return probe_generator_parallel(
        filepath=probes_filepath,
        url=database_url,
        measurement_id=measurement_id,
        round_=next_round.number,
        mapper_v4=flow_mapper_v4,
        mapper_v6=flow_mapper_v6,
        probe_src_port=tool_parameters.initial_source_port,
        probe_dst_port=tool_parameters.destination_port,
        probe_ttl_geq=probe_ttl_geq,
        probe_ttl_leq=probe_ttl_leq,
    )


async def probes_inner_pipeline(
    clickhouse: ClickHouse,
    logger: Logger,
    # NOTE: Ideally we would not need to pass the strs here,
    # but rather directly a database/table.
    measurement_uuid: str,
    agent_uuid: str,
    _agent_min_ttl: int,
    _measurement_tags: List[str],
    # NOTE: Ideally the sliding window parameters would be tool parameters.
    # Iris shouldn't need to know about this feature.
    _sliding_window_stopping_condition: int,
    _tool: Tool,
    tool_parameters: ToolParameters,
    results_filepath: Optional[Path],
    targets_filepath: Path,
    probes_filepath: Path,
    previous_round: Optional[Round],
    _next_round: Round,
) -> int:
    """
    Given a targets file and an optional results file, write the probes for the next round.
    This is a generic implementation for the tools based on the diamond-miner library:
    diamond-miner, yarrp and ping.

    :returns: The number of probes written.
    """

    def log(s):
        logger.info(f"{measurement_uuid} :: {agent_uuid} :: {s}")

    if results_filepath:
        insert_results = InsertResults(
            clickhouse,
            measurement_uuid,
            agent_uuid,
            tool_parameters.prefix_len_v4,
            tool_parameters.prefix_len_v6,
        )
        log("Create results tables")
        await insert_results.create_table()
        log("Insert results file")
        await insert_results.insert_csv(results_filepath)
        log("Insert prefixes")
        await insert_results.insert_prefixes()
        log("Insert links")
        await insert_results.insert_links()

    if not previous_round:
        # This is the first round
        # Copy the target_file to the probes file.
        log("Copy targets file to probes file")
        ctx = ZstdDecompressor()
        with targets_filepath.open("rb") as inp:
            with probes_filepath.open("wb") as out:
                ctx.copy_stream(inp, out)

        # Count the number of probes (i.e., the number of line in the probe file)
        # in order to be compliant with the default inner pipeline
        return int(subprocess.check_output(["wc", "-l", targets_filepath]).split()[0])
    else:
        return 0


def instantiate_flow_mappers(
    klass: str, kwargs: dict, prefix_size_v4: int, prefix_size_v6: int
) -> Tuple[FlowMapper, FlowMapper]:
    flow_mapper_cls = getattr(mappers, klass)
    flow_mapper_kwargs = kwargs
    flow_mapper_v4 = flow_mapper_cls(
        **{"prefix_size": prefix_size_v4, **flow_mapper_kwargs}
    )
    flow_mapper_v6 = flow_mapper_cls(
        **{"prefix_size": prefix_size_v6, **flow_mapper_kwargs}
    )
    return flow_mapper_v4, flow_mapper_v6


inner_pipeline_for_tool: Dict[Tool, Callable] = defaultdict(
    lambda: default_inner_pipeline
)
inner_pipeline_for_tool[Tool.Probes] = probes_inner_pipeline
