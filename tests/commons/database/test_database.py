import pytest


@pytest.mark.asyncio
async def test_database(database):
    assert await database.call("SELECT 1 AS A") == [{"A": 1}]
