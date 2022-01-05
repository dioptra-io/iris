from uuid import uuid4

import pytest

from iris.commons.test import compress_file

pytestmark = pytest.mark.asyncio


async def test_insert_results(clickhouse, tmp_path):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())

    results_file = tmp_path / "results.csv"
    results_file.write_text(
        """capture_timestamp,probe_protocol,probe_src_addr,probe_dst_addr,probe_src_port,probe_dst_port,probe_ttl,quoted_ttl,reply_src_addr,reply_protocol,reply_icmp_type,reply_icmp_code,reply_ttl,reply_size,reply_mpls_labels,rtt,round
1640006077,1,::ffff:172.17.0.2,::ffff:62.40.124.69,24000,0,1,1,::ffff:172.17.0.1,1,11,0,64,59,"[]",1,1
1640006077,1,::ffff:172.17.0.2,::,24000,0,64,0,::ffff:62.40.124.69,1,0,0,254,94,"[]",28524,1
1640006077,1,::ffff:172.17.0.2,::,24000,0,5,0,::ffff:62.40.124.69,1,0,0,254,35,"[]",28524,1
"""
    )
    compress_file(results_file, results_file.with_suffix(".csv.zst"))

    assert (
        await clickhouse.create_tables(measurement_uuid, agent_uuid, 24, 64, drop=True)
        is None
    )
    assert (
        await clickhouse.insert_csv(
            measurement_uuid, agent_uuid, results_file.with_suffix(".csv.zst")
        )
        is None
    )
    assert await clickhouse.insert_prefixes(measurement_uuid, agent_uuid) is None
    assert await clickhouse.insert_links(measurement_uuid, agent_uuid) is None
    assert await clickhouse.grant_public_access(measurement_uuid, agent_uuid) is None
