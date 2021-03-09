"""Test of `profile` operation."""

import uuid

import iris.commons.database

# --- GET /v0/profile ---


def test_get_profile_no_ripe(client, monkeypatch):
    """Test get profile with no RIPE profile."""
    user_uuid = str(uuid.uuid4())

    async def fake_get(self, userrname):
        return {
            "uuid": user_uuid,
            "username": "test",
            "email": "test@test",
            "hashed_password": "hashed",
            "is_active": True,
            "is_admin": False,
            "is_full_capable": False,
            "register_date": "date",
            "ripe_account": None,
            "ripe_key": None,
        }

    monkeypatch.setattr(iris.commons.database.DatabaseUsers, "get", fake_get)

    response = client.get("/v0/profile")
    assert response.json() == {
        "uuid": user_uuid,
        "username": "test",
        "email": "test@test",
        "is_active": True,
        "is_admin": False,
        "is_full_capable": False,
        "register_date": "date",
        "ripe": {"account": None, "key": None},
    }


def test_get_profile_ripe(client, monkeypatch):
    """Test get profile with RIPE information."""
    user_uuid = str(uuid.uuid4())

    async def fake_get(self, userrname):
        return {
            "uuid": user_uuid,
            "username": "test",
            "email": "test@test",
            "hashed_password": "hashed",
            "is_active": True,
            "is_admin": False,
            "is_full_capable": False,
            "register_date": "date",
            "ripe_account": "test",
            "ripe_key": "key",
        }

    monkeypatch.setattr(iris.commons.database.DatabaseUsers, "get", fake_get)

    response = client.get("/v0/profile")
    assert response.json() == {
        "uuid": user_uuid,
        "username": "test",
        "email": "test@test",
        "is_active": True,
        "is_admin": False,
        "is_full_capable": False,
        "register_date": "date",
        "ripe": {"account": "test", "key": "key"},
    }


# --- PUT /v0/profile/ripe ---


def test_put_profile_ripe(client, monkeypatch):
    """Test of put RIPE profile."""

    async def fake_register_ripe(*args, **kwargs):
        return

    monkeypatch.setattr(
        iris.commons.database.DatabaseUsers, "register_ripe", fake_register_ripe
    )
    response = client.put("/v0/profile/ripe", json={"account": "test", "key": "test"})
    assert response.json() == {"account": "test", "key": "test"}


def test_put_profile_ripe_clear(client, monkeypatch):
    """Test of clear RIPE profile."""

    async def fake_register_ripe(*args, **kwargs):
        return

    monkeypatch.setattr(
        iris.commons.database.DatabaseUsers, "register_ripe", fake_register_ripe
    )
    response = client.put("/v0/profile/ripe", json={"account": None, "key": None})
    assert response.json() == {"account": None, "key": None}


def test_put_profile_ripe_invalid_input(client, monkeypatch):
    """Test of put RIPE profile with invalid input."""

    async def fake_register_ripe(*args, **kwargs):
        return

    monkeypatch.setattr(
        iris.commons.database.DatabaseUsers, "register_ripe", fake_register_ripe
    )

    response = client.put("/v0/profile/ripe", json={"account": "test", "key": None})
    assert response.status_code == 422

    response = client.put("/v0/profile/ripe", json={"account": None, "key": "test"})
    assert response.status_code == 422
