import pytest

import iris.commons.database.users
from iris.commons.schemas.public import Profile, RIPEAccount


async def dummy(*args, **kwargs):
    return


def fake_get_user(user):
    async def get_user(*args, **kwargs):
        return user

    return get_user


# --- POST /api/profile/token


@pytest.mark.asyncio
async def test_post_profile_token(api_client, user, monkeypatch):
    monkeypatch.setattr(iris.api.security, "get_user", fake_get_user(user))
    async with api_client as c:
        response = await c.post(
            "/api/profile/token", data={"username": user.username, "password": "test"}
        )
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_post_profile_token_bad_credentials(api_client, user, monkeypatch):
    monkeypatch.setattr(iris.api.security, "get_user", fake_get_user(user))
    async with api_client as c:
        response = await c.post(
            "/api/profile/token", data={"username": user.username, "password": "toto"}
        )
        assert response.status_code == 401


@pytest.mark.asyncio
async def test_post_profile_token_inactive(api_client, user, monkeypatch):
    user = user.copy(update={"is_active": False})
    monkeypatch.setattr(iris.api.security, "get_user", fake_get_user(user))
    async with api_client as c:
        response = await c.post(
            "/api/profile/token", data={"username": "test", "password": "test"}
        )
        assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_profile_inactive(api_client_factory, user, monkeypatch):
    user = user.copy(update={"is_active": False})
    monkeypatch.setattr(iris.api.security, "get_user", fake_get_user(user))
    async with api_client_factory(override_user=None) as c:
        response = await c.get("/api/profile")
        assert response.status_code == 401


# # --- GET /api/profile ---


@pytest.mark.asyncio
async def test_get_profile_no_ripe(api_client_factory, user):
    user = user.copy(update={"ripe": None})
    async with api_client_factory(override_user=user) as c:
        response = await c.get("/api/profile")
        assert Profile(**response.json()) == user


@pytest.mark.asyncio
async def test_get_profile_ripe(api_client_factory, user):
    ripe = RIPEAccount(account="ripe-account", key="ripe-key")
    user = user.copy(update={"ripe": ripe})
    async with api_client_factory(override_user=user) as c:
        response = await c.get("/api/profile")
        assert Profile(**response.json()) == user


# --- PUT /api/profile/ripe ---


@pytest.mark.asyncio
async def test_put_profile_ripe(api_client, monkeypatch):
    monkeypatch.setattr(iris.commons.database.users.Users, "register_ripe", dummy)
    ripe = RIPEAccount(account="ripe-account", key="ripe-key")
    async with api_client as c:
        response = await c.put("/api/profile/ripe", json=ripe.dict())
        assert RIPEAccount(**response.json()) == ripe


@pytest.mark.asyncio
async def test_put_profile_ripe_clear(api_client, monkeypatch):
    monkeypatch.setattr(iris.commons.database.users.Users, "deregister_ripe", dummy)
    async with api_client as c:
        response = await c.delete("/api/profile/ripe")
        assert response.status_code == 200
