from ipaddress import IPv4Network, ip_network
from logging import Logger
from pathlib import Path
from typing import Optional, Tuple
from uuid import UUID

from diamond_miner import mappers
from diamond_miner.generators import probe_generator_parallel
from diamond_miner.insert import insert_mda_probe_counts_parallel, insert_probe_counts
from diamond_miner.queries import GetSlidingPrefixes
from diamond_miner.typing import FlowMapper

from iris.commons.database import Database, InsertResults
from iris.commons.schemas.public import Round, Tool, ToolParameters
from iris.worker.tree import load_targets


async def default_inner_pipeline(
    database: Database,
    logger: Logger,
    # NOTE: Ideally we would not need to pass the UUIDs here,
    # but rather directly a database/table.
    measurement_uuid: UUID,
    agent_uuid: UUID,
    agent_min_ttl: int,
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
        logger.info(f"{measurement_uuid} :: {agent_uuid} {s}")

    database_url = database.settings.database_url()
    measurement_id = f"{measurement_uuid}__{agent_uuid}"

    flow_mapper_v4, flow_mapper_v6 = instantiate_flow_mappers(
        tool_parameters.flow_mapper,
        tool_parameters.flow_mapper_kwargs or {},
        tool_parameters.prefix_size_v4,
        tool_parameters.prefix_size_v6,
    )

    if results_filepath:
        insert_results = InsertResults(
            database,
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
                net = ip_network(prefix)
                if isinstance(net, IPv4Network):
                    subnets = net.subnets(new_prefix=tool_parameters.prefix_len_v4)
                else:
                    subnets = net.subnets(new_prefix=tool_parameters.prefix_len_v6)
                for protocol, ttls, n_initial_flows in targets[prefix]:
                    for subnet in subnets:
                        prefixes.append((str(subnet), protocol, ttls, n_initial_flows))
        else:
            log("Enumerate sliding prefixes")
            query = GetSlidingPrefixes(
                window_max_ttl=previous_round.max_ttl,
                stopping_condition=sliding_window_stopping_condition,
            )
            async for _, _, addr_v6 in query.execute_iter_async(
                database_url, measurement_id
            ):
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
        await insert_mda_probe_counts_parallel(
            url=database_url,
            measurement_id=measurement_id,
            previous_round=previous_round.number,
            target_epsilon=tool_parameters.failure_rate,
            adaptive_eps=True,
        )

    log("Generate probes file")
    return await probe_generator_parallel(
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
