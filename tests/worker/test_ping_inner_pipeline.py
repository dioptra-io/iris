from uuid import uuid4

from iris.commons.models.diamond_miner import ToolParameters
from iris.commons.models.round import Round
from iris.commons.test import decompress_file
from iris.worker.inner_pipeline.ping import ping_inner_pipeline


async def test_ping_inner_pipeline(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())
    await clickhouse.create_tables(measurement_uuid, agent_uuid, 24, 64, drop=True)

    probes_filepath = tmp_path / "probes.csv.zst"
    targets_filepath = tmp_path / "targets.csv"
    targets_filepath.write_text("1.0.0.0/23,icmp,0,32,6")

    n_probes = await ping_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=0,
        measurement_tags=["public"],
        sliding_window_stopping_condition=3,
        tool_parameters=ToolParameters(),
        results_filepath=None,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=None,
        next_round=Round(number=1, limit=10, offset=0),
        max_open_files=128,
    )

    probes_filepath = decompress_file(probes_filepath)
    probes = probes_filepath.read_text().split()
    assert len(probes) == n_probes == 12
