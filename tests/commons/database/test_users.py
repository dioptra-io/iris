import pytest

from iris.commons.database import users
from iris.commons.schemas.public import RIPEAccount


@pytest.mark.asyncio
async def test_users(database, user):
    await users.create_table(database, drop=True)
    await users.register(database, user)
    assert not await users.get(database, "unknown")

    res = await users.get(database, user.username)
    assert res == user

    ripe_account = RIPEAccount(account="ripe-account", key="ripe-key")

    await users.register_ripe(database, user.username, ripe_account)
    res = await users.get(database, user.username)
    assert res.ripe == ripe_account

    await users.deregister_ripe(database, user.username)
    res = await users.get(database, user.username)
    assert not res.ripe
