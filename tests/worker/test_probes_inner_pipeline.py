from uuid import uuid4

from iris.commons.models.diamond_miner import ToolParameters
from iris.commons.models.round import Round
from iris.commons.test import compress_file, decompress_file
from iris.worker.inner_pipeline import probes_inner_pipeline


async def test_probes_inner_pipeline(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())

    probes_filepath = tmp_path / "probes_out.csv.zst"
    targets_filepath = tmp_path / "probes_inp.csv"
    targets_filepath.write_text("1,2,3,4\na,b,c,d\n")

    n_probes = await probes_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=0,
        sliding_window_stopping_condition=0,
        tool_parameters=ToolParameters(),
        results_filepath=None,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=None,
        next_round=Round(number=1, limit=10, offset=0),
        max_open_files=8192,
    )
    assert n_probes == 2
    probes_filepath = decompress_file(probes_filepath)
    assert probes_filepath.read_text() == targets_filepath.read_text()


async def test_probes_inner_pipeline_results(clickhouse, logger, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())

    probes_filepath = tmp_path / "probes_out.csv.zst"
    results_filepath = tmp_path / "results.csv"
    results_filepath.write_text(
        """capture_timestamp,probe_protocol,probe_src_addr,probe_dst_addr,probe_src_port,probe_dst_port,probe_ttl,quoted_ttl,reply_src_addr,reply_protocol,reply_icmp_type,reply_icmp_code,reply_ttl,reply_size,reply_mpls_labels,rtt,round
1640006077,1,::ffff:172.17.0.2,::ffff:1.0.0.1,24000,0,9,1,::ffff:80.80.80.80,1,11,0,64,59,"[]",1,1
1640006077,58,2002::1,2001::1,24000,0,9,1,2003::1,58,3,0,64,59,"[]",1,1
"""
    )
    results_filepath = compress_file(results_filepath)
    targets_filepath = tmp_path / "probes_inp.csv"
    targets_filepath.write_text("1,2,3,4\na,b,c,d\n")

    n_probes = await probes_inner_pipeline(
        clickhouse=clickhouse,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        agent_min_ttl=0,
        sliding_window_stopping_condition=0,
        tool_parameters=ToolParameters(),
        results_filepath=results_filepath,
        targets_filepath=targets_filepath,
        probes_filepath=probes_filepath,
        previous_round=Round(number=1, limit=10, offset=0),
        next_round=Round(number=2, limit=0, offset=0),
        max_open_files=8192,
    )

    assert n_probes == 0
    assert not probes_filepath.exists()
