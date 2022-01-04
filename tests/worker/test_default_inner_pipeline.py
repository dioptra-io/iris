from uuid import uuid4

import pytest

from iris.commons.models.diamond_miner import Tool, ToolParameters
from iris.commons.models.round import Round
from iris.commons.results import InsertResults
from iris.commons.test import decompress_file
from iris.worker.inner_pipeline import default_inner_pipeline

pytestmark = pytest.mark.asyncio

# TODO: Refactor tests


async def test_default_inner_pipeline_round_1_0(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())

    await InsertResults(
        clickhouse=clickhouse,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        prefix_len_v4=24,
        prefix_len_v6=64,
    ).create_table(drop=True)

    probes_filepath = tmp_path / "probes.csv.zst"
    targets_filepath = tmp_path / "targets.csv"
    targets_filepath.write_text("1.0.0.0/23,icmp,2,32,6")

    n_probes = await default_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=2,
        measurement_tags=["public"],
        sliding_window_stopping_condition=3,
        tool=Tool.DiamondMiner,
        tool_parameters=ToolParameters(),
        results_filepath=None,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=None,
        next_round=Round(number=1, limit=10, offset=0),
    )

    decompress_file(probes_filepath, probes_filepath.with_suffix(".csv"))
    probes = probes_filepath.with_suffix(".csv").read_text().split()
    assert len(probes) == n_probes == 108


async def test_default_inner_pipeline_round_1_1_no_results(
    clickhouse, logger, tmp_path
):
    # TODO: Document test, here we do not insert any results, so stopping condition = 3, so ...
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())

    await InsertResults(
        clickhouse=clickhouse,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        prefix_len_v4=24,
        prefix_len_v6=64,
    ).create_table(drop=True)

    probes_filepath = tmp_path / "probes.csv.zst"
    targets_filepath = tmp_path / "targets.csv"
    targets_filepath.write_text("1.0.0.0/23,icmp,2,32,6")

    n_probes = await default_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=2,
        measurement_tags=["public"],
        sliding_window_stopping_condition=3,
        tool=Tool.DiamondMiner,
        tool_parameters=ToolParameters(),
        results_filepath=None,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=Round(number=1, limit=10, offset=0),
        next_round=Round(number=1, limit=10, offset=1),
    )
    assert n_probes == 0
    assert not probes_filepath.exists()


async def test_default_inner_pipeline_round_1_1_results(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())

    # TODO: Refactor this or rename, InsertResults is confusing.
    # TODO: Merge with ClickHouse class?
    # clickhouse.create_tables(measurement_uuid, agent_uuid, drop=True)
    await InsertResults(
        clickhouse=clickhouse,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        prefix_len_v4=24,
        prefix_len_v6=64,
    ).create_table(drop=True)

    probes_filepath = tmp_path / "probes.csv.zst"
    # TODO: Create results filepath
    targets_filepath = tmp_path / "targets.csv"
    targets_filepath.write_text("1.0.0.0/23,icmp,2,32,6")

    n_probes = await default_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=2,
        measurement_tags=["public"],
        sliding_window_stopping_condition=3,
        tool=Tool.DiamondMiner,
        tool_parameters=ToolParameters(),
        results_filepath=None,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=Round(number=1, limit=10, offset=0),
        next_round=Round(number=1, limit=10, offset=1),
    )

    decompress_file(probes_filepath, probes_filepath.with_suffix(".csv"))
    probes = probes_filepath.with_suffix(".csv").read_text().split()
    assert len(probes) == n_probes == 108


async def test_default_inner_pipeline_round_2(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())

    await InsertResults(
        clickhouse=clickhouse,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        prefix_len_v4=24,
        prefix_len_v6=64,
    ).create_table(drop=True)

    probes_filepath = tmp_path / "probes.csv.zst"
    targets_filepath = tmp_path / "targets.csv"
    targets_filepath.write_text("1.0.0.0/23,icmp,2,32,6")

    n_probes = await default_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=2,
        measurement_tags=["public"],
        sliding_window_stopping_condition=3,
        tool=Tool.DiamondMiner,
        tool_parameters=ToolParameters(),
        results_filepath=None,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=Round(number=1, limit=10, offset=1),
        next_round=Round(number=2, limit=0, offset=0),
    )

    decompress_file(probes_filepath, probes_filepath.with_suffix(".csv"))
    probes = probes_filepath.with_suffix(".csv").read_text().split()
    assert len(probes) == n_probes == 108
