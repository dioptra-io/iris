from uuid import uuid4

import pytest

from iris.commons.results import InsertResults
from iris.commons.test import compress_file

pytestmark = pytest.mark.asyncio


async def test_measurement_results(clickhouse, tmp_path):
    db = InsertResults(clickhouse, str(uuid4()), str(uuid4()), 24, 64)

    results_file = tmp_path / "results.csv"
    results_file.write_text(
        """capture_timestamp,probe_protocol,probe_src_addr,probe_dst_addr,probe_src_port,probe_dst_port,probe_ttl,quoted_ttl,reply_src_addr,reply_protocol,reply_icmp_type,reply_icmp_code,reply_ttl,reply_size,reply_mpls_labels,rtt,round
1640006077,1,::ffff:172.17.0.2,::ffff:62.40.124.69,24000,0,1,1,::ffff:172.17.0.1,1,11,0,64,59,"[]",1,1
1640006077,1,::ffff:172.17.0.2,::,24000,0,64,0,::ffff:62.40.124.69,1,0,0,254,94,"[]",28524,1
1640006077,1,::ffff:172.17.0.2,::,24000,0,5,0,::ffff:62.40.124.69,1,0,0,254,35,"[]",28524,1
"""
    )
    compress_file(results_file, results_file.with_suffix(".csv.zst"))

    assert await db.create_table(drop=True) is None
    assert await db.insert_csv(results_file.with_suffix(".csv.zst")) is None
    assert await db.insert_prefixes() is None
    assert await db.insert_links() is None
