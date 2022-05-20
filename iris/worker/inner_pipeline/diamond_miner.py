from ipaddress import ip_address
from logging import Logger
from pathlib import Path

from diamond_miner import mappers
from diamond_miner.generators import probe_generator_parallel
from diamond_miner.insert import insert_mda_probe_counts, insert_probe_counts
from diamond_miner.queries import GetSlidingPrefixes
from diamond_miner.typing import FlowMapper
from pych_client import ClickHouseClient

from iris.commons.clickhouse import ClickHouse
from iris.commons.models import Round, ToolParameters
from iris.worker.tree import load_targets


async def diamond_miner_inner_pipeline(
    clickhouse: ClickHouse,
    logger: Logger,
    # NOTE: Ideally we would not need to pass the UUIDs here,
    # but rather directly a database/table.
    measurement_uuid: str,
    agent_uuid: str,
    agent_min_ttl: int,
    # NOTE: Ideally the sliding window parameters would be tool parameters.
    # Iris shouldn't need to know about this feature.
    sliding_window_stopping_condition: int,
    tool_parameters: ToolParameters,
    results_filepath: Path | None,
    targets_filepath: Path,
    probes_filepath: Path,
    previous_round: Round | None,
    next_round: Round,
    max_open_files: int,
) -> int:
    """
    Given a targets file and an optional results file, write the probes for the next round.
    This is a generic implementation for the tools based on the diamond-miner library:
    diamond-miner and yarrp.

    :returns: The number of probes written.
    """
    client = ClickHouseClient(**clickhouse.settings.clickhouse)
    measurement_id = f"{measurement_uuid}__{agent_uuid}"

    flow_mapper_v4, flow_mapper_v6 = instantiate_flow_mappers(
        tool_parameters.flow_mapper.value,
        tool_parameters.flow_mapper_kwargs or {},
        tool_parameters.prefix_size_v4,
        tool_parameters.prefix_size_v6,
    )

    await clickhouse.create_tables(
        measurement_uuid,
        agent_uuid,
        tool_parameters.prefix_len_v4,
        tool_parameters.prefix_len_v6,
    )

    if results_filepath:
        await clickhouse.insert_csv(measurement_uuid, agent_uuid, results_filepath)
        await clickhouse.insert_prefixes(measurement_uuid, agent_uuid)
        await clickhouse.insert_links(measurement_uuid, agent_uuid)

    probe_ttl_geq = 0
    probe_ttl_leq = 255

    # Compute the sub-rounds of round 1.
    if next_round.number == 1:
        probe_ttl_geq = max(agent_min_ttl, next_round.min_ttl)
        probe_ttl_leq = next_round.max_ttl
        logger.info(
            f"Next round window: TTL {probe_ttl_geq} to {probe_ttl_leq} (incl.)"
        )

        logger.info("Load targets")
        with targets_filepath.open() as f:
            targets = load_targets(
                f,
                clamp_ttl_min=probe_ttl_geq,
                clamp_ttl_max=probe_ttl_leq,
            )

        logger.info("Compute the prefixes to probe")
        prefixes = []

        if previous_round is None:
            logger.info("Enumerate initial prefixes")
            for prefix in targets:
                for protocol, ttls, n_initial_flows in targets[prefix]:
                    prefixes.append((prefix, protocol, ttls, n_initial_flows))
        else:
            logger.info("Enumerate sliding prefixes")
            query = GetSlidingPrefixes(
                window_max_ttl=previous_round.max_ttl,
                stopping_condition=sliding_window_stopping_condition,
            )
            for row in query.execute_iter(client, measurement_id):
                addr_v6 = ip_address(row["probe_dst_prefix"])
                if addr_v4 := addr_v6.ipv4_mapped:
                    prefix = f"{addr_v4}/{tool_parameters.prefix_len_v4}"
                else:
                    prefix = f"{addr_v6}/{tool_parameters.prefix_len_v6}"
                for protocol, ttls, n_initial_flows in targets[prefix]:
                    prefixes.append((prefix, protocol, ttls, n_initial_flows))

        logger.info("Insert probe counts")
        insert_probe_counts(
            client=client,
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
        logger.info("Insert MDA probe counts")
        insert_mda_probe_counts(
            client=client,
            measurement_id=measurement_id,
            previous_round=previous_round.number,
            target_epsilon=tool_parameters.failure_probability,
            adaptive_eps=True,
        )

    logger.info("Generate probes file")
    return probe_generator_parallel(
        filepath=probes_filepath,
        client=client,
        measurement_id=measurement_id,
        round_=next_round.number,
        mapper_v4=flow_mapper_v4,
        mapper_v6=flow_mapper_v6,
        probe_src_port=tool_parameters.initial_source_port,
        probe_dst_port=tool_parameters.destination_port,
        probe_ttl_geq=probe_ttl_geq,
        probe_ttl_leq=probe_ttl_leq,
        max_open_files=max_open_files,
    )


def instantiate_flow_mappers(
    klass: str, kwargs: dict, prefix_size_v4: int, prefix_size_v6: int
) -> tuple[FlowMapper, FlowMapper]:
    flow_mapper_cls = getattr(mappers, klass)
    flow_mapper_kwargs = kwargs
    flow_mapper_v4 = flow_mapper_cls(
        **{"prefix_size": prefix_size_v4, **flow_mapper_kwargs}
    )
    flow_mapper_v6 = flow_mapper_cls(
        **{"prefix_size": prefix_size_v6, **flow_mapper_kwargs}
    )
    return flow_mapper_v4, flow_mapper_v6
