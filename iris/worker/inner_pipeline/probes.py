import subprocess
from logging import Logger
from pathlib import Path
from typing import List, Optional

from zstandard import ZstdDecompressor

from iris.commons.clickhouse import ClickHouse
from iris.commons.models.diamond_miner import Tool, ToolParameters
from iris.commons.models.round import Round


async def probes_inner_pipeline(
    clickhouse: ClickHouse,
    logger: Logger,
    # NOTE: Ideally we would not need to pass the strs here,
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
) -> int:
    """
    :returns: The number of probes written.
    """

    if results_filepath:
        await clickhouse.create_tables(
            measurement_uuid,
            agent_uuid,
            tool_parameters.prefix_len_v4,
            tool_parameters.prefix_len_v6,
        )
        await clickhouse.insert_csv(measurement_uuid, agent_uuid, results_filepath)
        await clickhouse.insert_prefixes(measurement_uuid, agent_uuid)
        await clickhouse.insert_links(measurement_uuid, agent_uuid)

    if previous_round:
        # Probes tool has only one round.
        return 0

    # Copy the target_file to the probes file.
    logger.info("Copy targets file to probes file")
    ctx = ZstdDecompressor()
    with targets_filepath.open("rb") as inp:
        with probes_filepath.open("wb") as out:
            ctx.copy_stream(inp, out)

    # Count the number of probes (i.e., the number of line in the probe file)
    # in order to be compliant with the default inner pipeline
    return int(subprocess.check_output(["wc", "-l", targets_filepath]).split()[0])
