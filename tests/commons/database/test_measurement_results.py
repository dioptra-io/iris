import logging
import uuid
from subprocess import run

import pytest

from iris.commons.database import MeasurementResults
from iris.commons.settings import CommonSettings

settings = CommonSettings(DATABASE_HOST="localhost")


@pytest.mark.asyncio
async def test_measurement_results(common_settings, tmp_path):
    db = MeasurementResults(
        common_settings, logging.getLogger(__name__), uuid.uuid4(), uuid.uuid4()
    )
    assert await db.create_database() is None

    results_file = tmp_path / "results.csv"
    results_file.write_text(
        '1,::ffff:10.31.46.69,::ffff:8.8.8.8,24000,0,4,1,::ffff:20.20.20.1,1,11,0,252,62,"[]",9.0,1'
    )
    run(["zstd", results_file], check=True)

    assert await db.create_table(drop=True) is None
    assert await db.exists()

    assert await db.insert_csv(results_file.with_suffix(".csv.zst")) is None
    assert await db.insert_prefixes() is None
    assert await db.insert_links() is None

    assert await db.all_count() == 1
    assert await db.all(0, 10) == [
        {
            "probe_protocol": 1,
            "probe_src_addr": "::ffff:a1f:2e45",
            "probe_dst_addr": "::ffff:808:808",
            "probe_src_port": 24000,
            "probe_dst_port": 0,
            "probe_ttl": 4,
            "quoted_ttl": 1,
            "reply_src_addr": "::ffff:1414:1401",
            "reply_protocol": 1,
            "reply_icmp_type": 11,
            "reply_icmp_code": 0,
            "reply_ttl": 252,
            "reply_size": 62,
            "reply_mpls_labels": [],
            "rtt": 9.0,
            "round": 1,
        }
    ]


@pytest.mark.asyncio
async def test_measurement_results_sequential(common_settings, tmp_path):
    common_settings.DATABASE_PARALLEL_CSV_INSERT = False

    db = MeasurementResults(
        common_settings, logging.getLogger(__name__), uuid.uuid4(), uuid.uuid4()
    )
    assert await db.create_database() is None

    results_file = tmp_path / "results.csv"
    results_file.write_text(
        '1,::ffff:10.31.46.69,::ffff:8.8.8.8,24000,0,4,1,::ffff:20.20.20.1,1,11,0,252,62,"[]",9.0,1'
    )
    run(["zstd", results_file], check=True)

    assert await db.create_table(drop=True) is None
    assert await db.insert_csv(results_file.with_suffix(".csv.zst")) is None
    assert await db.insert_prefixes() is None
    assert await db.insert_links() is None
    assert await db.all_count() == 1
