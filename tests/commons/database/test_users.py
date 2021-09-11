import pytest

from iris.commons.database import users
from iris.commons.schemas.public import Profile, RIPEAccount


@pytest.mark.asyncio
async def test_users(database):
    await users.create_table(database, drop=True)

    user = Profile(
        username="foo",
        email="foo.bar@mail.com",
        is_active=True,
        is_admin=True,
        quota=100,
    )
    user._hashed_password = "abcdef"

    await users.register(database, user)
    assert not await users.get(database, "unknown")

    res = await users.get(database, user.username)
    assert res == user

    ripe_account = RIPEAccount(account="ripe-account", key="ripe-key")

    await users.register_ripe(database, user.username, ripe_account)
    res = await users.get(database, "foo")
    assert res.ripe == ripe_account

    await users.deregister_ripe(database, user.username)
    res = await users.get(database, "foo")
    assert not res.ripe
