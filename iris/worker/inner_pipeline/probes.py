import subprocess
from logging import Logger
from pathlib import Path
from typing import List, Optional

from zstandard import ZstdDecompressor

from iris.commons.clickhouse import ClickHouse
from iris.commons.models.diamond_miner import Tool, ToolParameters
from iris.commons.models.round import Round
from iris.commons.results import InsertResults


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
