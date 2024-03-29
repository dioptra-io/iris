import subprocess
from logging import Logger
from pathlib import Path

from zstandard import ZstdCompressor

from iris.commons.clickhouse import ClickHouse
from iris.commons.models import Round, ToolParameters


async def probes_inner_pipeline(
    clickhouse: ClickHouse,
    logger: Logger,
    # NOTE: Ideally we would not need to pass the strs here,
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
        # Probes tool has only one round.
        return 0

    # Copy the target file to the probes file.
    logger.info("Copy targets file to probes file")
    ctx = ZstdCompressor()
    with targets_filepath.open("rb") as inp:
        with probes_filepath.open("wb") as out:
            ctx.copy_stream(inp, out)

    # Count the number of probes (i.e., the number of line in the probe file)
    # in order to be compliant with the default inner pipeline
    return int(subprocess.check_output(["wc", "-l", targets_filepath]).split()[0])
