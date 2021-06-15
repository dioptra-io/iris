import logging
from datetime import datetime

import pytest

from iris.commons.database import Users


@pytest.mark.asyncio
async def test_users(common_settings):
    db = Users(common_settings, logging.getLogger(__name__))
    assert await db.create_database() is None
    assert await db.create_table(drop=True) is None

    data = {
        "username": "foo",
        "email": "foo.bar@mail.com",
        "hashed_password": "abcdef",
        "is_active": True,
        "is_admin": False,
        "quota": 100,
        "register_date": datetime(2021, 2, 1, 12, 50, 30),
    }

    assert await db.register(data) is None
    assert await db.get("unknown") is None

    res = await db.get("foo")
    assert res["uuid"] is not None
    assert res["username"] == data["username"]
    assert res["email"] == data["email"]
    assert res["hashed_password"] == data["hashed_password"]
    assert res["is_active"] == data["is_active"]
    assert res["is_admin"] == data["is_admin"]
    assert res["quota"] == data["quota"]
    assert res["register_date"] == data["register_date"].isoformat()
    assert res["ripe_account"] is None
    assert res["ripe_key"] is None

    assert await db.register_ripe("foo", "ripe-account", "ripe-key") is None
    res = await db.get("foo")
    assert res["ripe_account"] == "ripe-account"
    assert res["ripe_key"] == "ripe-key"

    assert await db.register_ripe("foo", None, None) is None
    res = await db.get("foo")
    assert res["ripe_account"] is None
    assert res["ripe_key"] is None
