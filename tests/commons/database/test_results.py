import uuid
from subprocess import run

import pytest

from iris.commons.database import InsertResults
from iris.commons.settings import CommonSettings

settings = CommonSettings(DATABASE_HOST="localhost")


@pytest.mark.asyncio
async def test_measurement_results(database, tmp_path):
    db = InsertResults(database, uuid.uuid4(), uuid.uuid4(), 24, 64)

    results_file = tmp_path / "results.csv"
    results_file.write_text(
        '1638530032054872,1,::ffff:10.31.46.69,::ffff:8.8.8.8,24000,0,4,1,::ffff:20.20.20.1,1,11,0,252,62,"[]",90,1'
        + '\n1638530032054872,1,::ffff:10.31.46.69,::ffff:8.8.8.8,24000,0,5,1,::ffff:20.20.20.2,1,11,0,252,62,"[]",90,1'
    )
    run([settings.ZSTD_CMD, results_file], check=True)

    assert await db.create_table(drop=True) is None
    assert await db.insert_csv(results_file.with_suffix(".csv.zst")) is None
    assert await db.insert_prefixes() is None
    assert await db.insert_links() is None
