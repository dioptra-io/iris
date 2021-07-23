import logging
import uuid
from subprocess import run

import pytest

from iris.commons.database import InsertResults, Interfaces, Links, Prefixes, Replies
from iris.commons.settings import CommonSettings

settings = CommonSettings(DATABASE_HOST="localhost")


@pytest.mark.asyncio
async def test_measurement_results(common_settings, tmp_path):
    db = InsertResults(
        common_settings, logging.getLogger(__name__), uuid.uuid4(), uuid.uuid4()
    )
    assert await db.create_database() is None

    results_file = tmp_path / "results.csv"
    results_file.write_text(
        '1,::ffff:10.31.46.69,::ffff:8.8.8.8,24000,0,4,1,::ffff:20.20.20.1,1,11,0,252,62,"[]",9.0,1'
        + '\n1,::ffff:10.31.46.69,::ffff:8.8.8.8,24000,0,5,1,::ffff:20.20.20.2,1,11,0,252,62,"[]",9.0,1'
    )
    run([settings.ZSTD_CMD, results_file], check=True)

    assert await db.create_table(drop=True) is None
    assert await db.insert_csv(results_file.with_suffix(".csv.zst")) is None
    assert await db.insert_prefixes() is None
    assert await db.insert_links() is None

    db = Replies(
        common_settings, logging.getLogger(__name__), db.measurement_uuid, db.agent_uuid
    )
    assert await db.exists()
    assert await db.all_count() == 2
    assert await db.all(0, 10) == [
        {
            "probe_protocol": "icmp",
            "probe_src_addr": "10.31.46.69",
            "probe_dst_addr": "8.8.8.8",
            "probe_src_port": 24000,
            "probe_dst_port": 0,
            "probe_ttl": 4,
            "quoted_ttl": 1,
            "reply_src_addr": "20.20.20.1",
            "reply_protocol": "icmp",
            "reply_icmp_type": 11,
            "reply_icmp_code": 0,
            "reply_ttl": 252,
            "reply_size": 62,
            "reply_mpls_labels": [],
            "rtt": 9.0,
            "round": 1,
        },
        {
            "probe_protocol": "icmp",
            "probe_src_addr": "10.31.46.69",
            "probe_dst_addr": "8.8.8.8",
            "probe_src_port": 24000,
            "probe_dst_port": 0,
            "probe_ttl": 5,
            "quoted_ttl": 1,
            "reply_src_addr": "20.20.20.2",
            "reply_protocol": "icmp",
            "reply_icmp_type": 11,
            "reply_icmp_code": 0,
            "reply_ttl": 252,
            "reply_size": 62,
            "reply_mpls_labels": [],
            "rtt": 9.0,
            "round": 1,
        },
    ]
    assert await db.all(2, 10) == []

    db = Interfaces(
        common_settings, logging.getLogger(__name__), db.measurement_uuid, db.agent_uuid
    )
    assert await db.exists()
    assert await db.all_count() == 2
    assert await db.all(0, 10) == [
        {"ttl": 4, "addr": "20.20.20.1"},
        {"ttl": 5, "addr": "20.20.20.2"},
    ]
    assert await db.all(2, 10) == []

    db = Links(
        common_settings, logging.getLogger(__name__), db.measurement_uuid, db.agent_uuid
    )
    assert await db.exists()
    assert await db.all_count() == 1
    assert await db.all(0, 10) == [
        {
            "near_ttl": 4,
            "far_ttl": 5,
            "near_addr": "20.20.20.1",
            "far_addr": "20.20.20.2",
        }
    ]
    assert await db.all(1, 10) == []

    db = Prefixes(
        common_settings, logging.getLogger(__name__), db.measurement_uuid, db.agent_uuid
    )
    assert await db.exists()
    assert await db.all_count() == 1
    assert await db.all(0, 10) == [
        {"prefix": "8.8.8.0", "has_amplification": False, "has_loops": False}
    ]
    assert await db.all(1, 10) == []
