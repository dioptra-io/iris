import uuid

import iris.commons.database.users
from iris.api.security import get_current_active_user

from ..conftest import override_get_current_active_user

# --- POST /api/profile/token


def test_post_profile_token(api_client_sync, monkeypatch):
    async def fake_get_user(*args, **kwargs):
        return override_get_current_active_user()

    monkeypatch.setattr(
        iris.api.security,
        "get_user",
        fake_get_user,
    )

    response = api_client_sync.post(
        "/api/profile/token", {"username": "test", "password": "test"}
    )
    assert response.status_code == 200


def test_post_profile_token_bad_credentials(api_client_sync, monkeypatch):
    async def fake_get_user(*args, **kwargs):
        return override_get_current_active_user()

    monkeypatch.setattr(
        iris.api.security,
        "get_user",
        fake_get_user,
    )

    response = api_client_sync.post(
        "/api/profile/token", {"username": "test", "password": "toto"}
    )
    assert response.status_code == 401


def test_post_profile_token_inactive(api_client_sync, monkeypatch):
    async def fake_get_user(*args, **kwargs):
        return {
            "uuid": "test",
            "username": "test",
            "email": "test@test",
            "hashed_password": (
                "$2y$12$seiW.kzNc9NFRlpQpyeKie.PUJGhAtxn6oGPB.XfgnmTKx8Y9XCve"
            ),
            "is_active": False,
            "is_admin": True,
            "quota": 1000,
            "register_date": "date",
            "ripe_account": None,
            "ripe_key": None,
        }

    monkeypatch.setattr(
        iris.api.security,
        "get_user",
        fake_get_user,
    )

    response = api_client_sync.post(
        "/api/profile/token", {"username": "test", "password": "test"}
    )
    assert response.status_code == 401


def test_get_profile_inactive(api_client_sync, monkeypatch):
    del api_client_sync.app.dependency_overrides[get_current_active_user]

    async def fake_get_user(*args, **kwargs):
        return {
            "uuid": "test",
            "username": "test",
            "email": "test@test",
            "hashed_password": (
                "$2y$12$seiW.kzNc9NFRlpQpyeKie.PUJGhAtxn6oGPB.XfgnmTKx8Y9XCve"
            ),
            "is_active": False,
            "is_admin": True,
            "quota": 1000,
            "register_date": "date",
            "ripe_account": None,
            "ripe_key": None,
        }

    monkeypatch.setattr(
        iris.api.security,
        "get_user",
        fake_get_user,
    )

    response = api_client_sync.get("/api/profile")
    assert response.status_code == 401

    # Reset back the override
    api_client_sync.app.dependency_overrides[
        get_current_active_user
    ] = override_get_current_active_user


# --- GET /api/profile ---


def test_get_profile_no_ripe(api_client_sync):
    user_uuid = str(uuid.uuid4())
    api_client_sync.app.dependency_overrides[get_current_active_user] = lambda: {
        "uuid": user_uuid,
        "username": "test",
        "email": "test@test",
        "is_active": True,
        "is_admin": False,
        "quota": 100,
        "register_date": "date",
        "ripe_account": None,
        "ripe_key": None,
    }

    response = api_client_sync.get("/api/profile")
    assert response.json() == {
        "uuid": user_uuid,
        "username": "test",
        "email": "test@test",
        "is_active": True,
        "is_admin": False,
        "quota": 100,
        "register_date": "date",
        "ripe": {"account": None, "key": None},
    }

    # Reset back the override
    api_client_sync.app.dependency_overrides[
        get_current_active_user
    ] = override_get_current_active_user


def test_get_profile_ripe(api_client_sync):
    user_uuid = str(uuid.uuid4())

    user_uuid = str(uuid.uuid4())
    api_client_sync.app.dependency_overrides[get_current_active_user] = lambda: {
        "uuid": user_uuid,
        "username": "test",
        "email": "test@test",
        "is_active": True,
        "is_admin": False,
        "quota": 100,
        "register_date": "date",
        "ripe_account": "test",
        "ripe_key": "key",
    }

    response = api_client_sync.get("/api/profile")
    assert response.json() == {
        "uuid": user_uuid,
        "username": "test",
        "email": "test@test",
        "is_active": True,
        "is_admin": False,
        "quota": 100,
        "register_date": "date",
        "ripe": {"account": "test", "key": "key"},
    }

    # Reset back the override
    api_client_sync.app.dependency_overrides[
        get_current_active_user
    ] = override_get_current_active_user


# --- PUT /api/profile/ripe ---


def test_put_profile_ripe(api_client_sync, monkeypatch):
    async def fake_register_ripe(*args, **kwargs):
        return

    monkeypatch.setattr(
        iris.commons.database.users.Users, "register_ripe", fake_register_ripe
    )
    response = api_client_sync.put(
        "/api/profile/ripe", json={"account": "test", "key": "test"}
    )
    assert response.json() == {"account": "test", "key": "test"}


def test_put_profile_ripe_clear(api_client_sync, monkeypatch):
    async def fake_register_ripe(*args, **kwargs):
        return

    monkeypatch.setattr(
        iris.commons.database.users.Users, "register_ripe", fake_register_ripe
    )
    response = api_client_sync.put(
        "/api/profile/ripe", json={"account": None, "key": None}
    )
    assert response.json() == {"account": None, "key": None}


def test_put_profile_ripe_invalid_input(api_client_sync, monkeypatch):
    async def fake_register_ripe(*args, **kwargs):
        return

    monkeypatch.setattr(
        iris.commons.database.users.Users, "register_ripe", fake_register_ripe
    )

    response = api_client_sync.put(
        "/api/profile/ripe", json={"account": "test", "key": None}
    )
    assert response.status_code == 422

    response = api_client_sync.put(
        "/api/profile/ripe", json={"account": None, "key": "test"}
    )
    assert response.status_code == 422
