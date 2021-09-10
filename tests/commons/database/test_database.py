import pytest


@pytest.mark.asyncio
async def test_database(database):
    assert await database.create_database() is None
    assert await database.call("SELECT 'A', 1") == [("A", 1)]
