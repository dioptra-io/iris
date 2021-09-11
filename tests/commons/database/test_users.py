import pytest

from iris.commons.database import Users
from iris.commons.schemas.public import Profile, RIPEAccount


@pytest.mark.asyncio
async def test_users(database):
    db = Users(database)
    await db.create_table(drop=True)

    user = Profile(
        username="foo",
        email="foo.bar@mail.com",
        is_active=True,
        is_admin=True,
        quota=100,
    )
    user._hashed_password = "abcdef"

    await db.register(user)
    assert not await db.get("unknown")

    res = await db.get(user.username)
    assert res == user

    ripe_account = RIPEAccount(account="ripe-account", key="ripe-key")

    await db.register_ripe(user.username, ripe_account)
    res = await db.get("foo")
    assert res.ripe == ripe_account

    await db.deregister_ripe(user.username)
    res = await db.get("foo")
    assert not res.ripe
