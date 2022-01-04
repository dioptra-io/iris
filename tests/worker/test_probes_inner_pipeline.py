from uuid import uuid4

import pytest

from iris.commons.models.diamond_miner import Tool, ToolParameters
from iris.commons.models.round import Round
from iris.commons.test import compress_file
from iris.worker.inner_pipeline import probes_inner_pipeline

pytestmark = pytest.mark.asyncio


async def test_probes_inner_pipeline(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())

    probes_filepath = tmp_path / "probes_out.csv"
    targets_filepath = tmp_path / "probes_inp.csv"
    targets_filepath.write_text("1,2,3,4\na,b,c,d\n")

    # probes_inner_pipeline expects a compressed input file.
    compress_file(targets_filepath, targets_filepath.with_suffix(".csv.zst"))

    n_probes = await probes_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        _agent_min_ttl=2,
        _measurement_tags=["public"],
        _sliding_window_stopping_condition=3,
        _tool=Tool.Probes,
        tool_parameters=ToolParameters(),
        results_filepath=None,
        targets_filepath=targets_filepath.with_suffix(".csv.zst"),
        probes_filepath=probes_filepath,
        previous_round=None,
        _next_round=Round(number=1, limit=10, offset=0),
    )
    assert n_probes == 2
    assert probes_filepath.read_text() == targets_filepath.read_text()
