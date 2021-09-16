import pytest

from iris.commons.database import users


@pytest.mark.asyncio
async def test_users(database, user):
    await users.create_table(database, drop=True)
    await users.register(database, user)
    assert not await users.get(database, "unknown")

    res = await users.get(database, user.username)
    assert res == user
