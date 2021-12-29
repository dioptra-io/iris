import pytest

pytestmark = pytest.mark.asyncio


async def test_database(clickhouse):
    assert await clickhouse.call("SELECT 1 AS A") == [{"A": 1}]
