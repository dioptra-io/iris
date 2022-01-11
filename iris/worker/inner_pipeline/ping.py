from logging import Logger
from pathlib import Path
from typing import List, Optional

from diamond_miner.generators import probe_generator_parallel
from diamond_miner.insert import insert_probe_counts

from iris.commons.clickhouse import ClickHouse
from iris.commons.models import Round, ToolParameters
from iris.worker.inner_pipeline.diamond_miner import instantiate_flow_mappers
from iris.worker.tree import load_targets


async def ping_inner_pipeline(
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
    tool_parameters: ToolParameters,
    results_filepath: Optional[Path],
    targets_filepath: Path,
    probes_filepath: Path,
    previous_round: Optional[Round],
    next_round: Round,
    max_open_files: int,
) -> int:
    """
    :returns: The number of probes written.
    """
    database_url = clickhouse.settings.CLICKHOUSE_URL
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

    if previous_round:
        # Ping tool has only one round.
        return 0

    logger.info("Load targets")
    with targets_filepath.open() as f:
        targets = load_targets(f)

    logger.info("Compute the prefixes to probe")
    prefixes = []
    for prefix in targets:
        for protocol, ttls, n_initial_flows in targets[prefix]:
            # In the case of ping, only take the max TTL in the TTL range.
            prefixes.append((prefix, protocol, (ttls[-1],), n_initial_flows))

    logger.info("Insert probe counts")
    insert_probe_counts(
        url=database_url,
        measurement_id=measurement_id,
        round_=next_round.number,
        prefixes=prefixes,
        prefix_len_v4=tool_parameters.prefix_len_v4,
        prefix_len_v6=tool_parameters.prefix_len_v6,
    )

    del prefixes, targets

    logger.info("Generate probes file")
    return probe_generator_parallel(
        filepath=probes_filepath,
        url=database_url,
        measurement_id=measurement_id,
        round_=next_round.number,
        mapper_v4=flow_mapper_v4,
        mapper_v6=flow_mapper_v6,
        probe_src_port=tool_parameters.initial_source_port,
        probe_dst_port=tool_parameters.destination_port,
        max_open_files=max_open_files,
    )
