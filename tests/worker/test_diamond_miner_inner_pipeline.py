from uuid import uuid4

import pytest

from iris.commons.models.diamond_miner import ToolParameters
from iris.commons.models.round import Round
from iris.commons.test import compress_file, decompress_file
from iris.worker.inner_pipeline import diamond_miner_inner_pipeline

pytestmark = pytest.mark.asyncio

targets_content = "1.0.0.0/23,icmp,0,32,6\n2001::/63,icmp6,0,32,6"


async def test_default_inner_pipeline_round_1_0(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())
    await clickhouse.create_tables(measurement_uuid, agent_uuid, 24, 64, drop=True)

    probes_filepath = tmp_path / "probes.csv.zst"
    targets_filepath = tmp_path / "targets.csv"
    targets_filepath.write_text(targets_content)

    n_probes = await diamond_miner_inner_pipeline(
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
    assert len(probes) == n_probes == 240


async def test_default_inner_pipeline_round_1_1_no_results(
    clickhouse, logger, tmp_path
):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())
    await clickhouse.create_tables(measurement_uuid, agent_uuid, 24, 64, drop=True)

    probes_filepath = tmp_path / "probes.csv.zst"
    targets_filepath = tmp_path / "targets.csv"
    targets_filepath.write_text(targets_content)

    n_probes = await diamond_miner_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=0,
        measurement_tags=["public"],
        sliding_window_stopping_condition=3,
        tool_parameters=ToolParameters(),
        # NOTE: here we do not insert any results, since we probed up to
        # TTL 10 during the previous round, and that the stopping condition
        # is 3 stars, we should not get any more probes.
        results_filepath=None,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=Round(number=1, limit=10, offset=0),
        next_round=Round(number=1, limit=10, offset=1),
        max_open_files=128,
    )
    assert n_probes == 0
    assert not probes_filepath.exists()


async def test_default_inner_pipeline_round_1_1_results(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())
    await clickhouse.create_tables(measurement_uuid, agent_uuid, 24, 64, drop=True)

    probes_filepath = tmp_path / "probes.csv.zst"
    results_filepath = tmp_path / "results.csv"
    results_filepath.write_text(
        """capture_timestamp,probe_protocol,probe_src_addr,probe_dst_addr,probe_src_port,probe_dst_port,probe_ttl,quoted_ttl,reply_src_addr,reply_protocol,reply_icmp_type,reply_icmp_code,reply_ttl,reply_size,reply_mpls_labels,rtt,round
1640006077,1,::ffff:172.17.0.2,::ffff:1.0.0.1,24000,0,9,1,::ffff:80.80.80.80,1,11,0,64,59,"[]",1,1
1640006077,58,2002::1,2001::1,24000,0,9,1,2003::1,58,3,0,64,59,"[]",1,1
"""
    )
    results_filepath = compress_file(results_filepath)
    targets_filepath = tmp_path / "targets.csv"
    targets_filepath.write_text(targets_content)

    n_probes = await diamond_miner_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=0,
        measurement_tags=["public"],
        sliding_window_stopping_condition=3,
        tool_parameters=ToolParameters(),
        # NOTE: here we insert results, so we should get probes from TTL 10 to 20,
        # only for 1.0.0.0/24 since we did not insert results for 1.0.1.0/24.
        # Same for 2001::/64
        results_filepath=results_filepath,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=Round(number=1, limit=10, offset=0),
        next_round=Round(number=1, limit=10, offset=1),
        max_open_files=128,
    )

    decompress_file(probes_filepath, probes_filepath.with_suffix(".csv"))
    probes = probes_filepath.with_suffix(".csv").read_text().split()
    assert len(probes) == n_probes == 120


async def test_default_inner_pipeline_round_2(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())
    await clickhouse.create_tables(measurement_uuid, agent_uuid, 24, 64, drop=True)

    probes_filepath = tmp_path / "probes.csv.zst"
    results_filepath = tmp_path / "results.csv"
    results_filepath.write_text(
        """capture_timestamp,probe_protocol,probe_src_addr,probe_dst_addr,probe_src_port,probe_dst_port,probe_ttl,quoted_ttl,reply_src_addr,reply_protocol,reply_icmp_type,reply_icmp_code,reply_ttl,reply_size,reply_mpls_labels,rtt,round
1640006077,1,::ffff:172.17.0.2,::ffff:1.0.0.1,24000,0,9,1,::ffff:80.80.80.80,1,11,0,64,59,"[]",1,1
1640006077,58,2002::1,2001::1,24000,0,9,1,2003::1,58,3,0,64,59,"[]",1,1
"""
    )
    results_filepath = compress_file(results_filepath)
    targets_filepath = tmp_path / "targets.csv"
    targets_filepath.write_text(targets_content)

    n_probes = await diamond_miner_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=2,
        measurement_tags=["public"],
        sliding_window_stopping_condition=3,
        tool_parameters=ToolParameters(),
        results_filepath=results_filepath,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=Round(number=1, limit=10, offset=1),
        next_round=Round(number=2, limit=0, offset=0),
        max_open_files=128,
    )
    # No load-balancing, so Diamond-Miner should stop here.
    assert n_probes == 0
    assert not probes_filepath.exists()
