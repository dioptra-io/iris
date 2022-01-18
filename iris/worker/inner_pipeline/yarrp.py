from logging import Logger
from pathlib import Path
from typing import List, Optional

from iris.commons.clickhouse import ClickHouse
from iris.commons.models import Round, ToolParameters
from iris.worker.inner_pipeline import diamond_miner_inner_pipeline


async def yarrp_inner_pipeline(
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
    Given a targets file and an optional results file, write the probes for the next round.
    This is a generic implementation for the tools based on the diamond-miner library:
    diamond-miner and yarrp.

    :returns: The number of probes written.
    """
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
        # Yarrp has only one round.
        return 0

    return await diamond_miner_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=agent_min_ttl,
        measurement_tags=measurement_tags,
        sliding_window_stopping_condition=sliding_window_stopping_condition,
        tool_parameters=tool_parameters,
        results_filepath=results_filepath,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=previous_round,
        next_round=next_round,
        max_open_files=max_open_files,
    )
