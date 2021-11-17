import pytest

import iris.api.security
from iris.commons.schemas.public import Profile


async def dummy(*args, **kwargs):
    return


def fake_get_user(user):
    async def get_user(*args, **kwargs):
        return user

    return get_user


# --- POST /profile/token


@pytest.mark.asyncio
async def test_post_profile_token(api_client, user, monkeypatch):
    monkeypatch.setattr(iris.api.security, "get_user", fake_get_user(user))
    async with api_client as c:
        response = await c.post(
            "/profile/token", data={"username": user.username, "password": "test"}
        )
        assert response.status_code == 200


@pytest.mark.asyncio
async def test_post_profile_token_bad_credentials(api_client, user, monkeypatch):
    monkeypatch.setattr(iris.api.security, "get_user", fake_get_user(user))
    async with api_client as c:
        response = await c.post(
            "/profile/token", data={"username": user.username, "password": "toto"}
        )
        assert response.status_code == 401


@pytest.mark.asyncio
async def test_post_profile_token_inactive(api_client, user, monkeypatch):
    user = user.copy(update={"is_active": False})
    monkeypatch.setattr(iris.api.security, "get_user", fake_get_user(user))
    async with api_client as c:
        response = await c.post(
            "/profile/token", data={"username": "test", "password": "test"}
        )
        assert response.status_code == 401


@pytest.mark.asyncio
async def test_get_profile_inactive(api_client_factory, user, monkeypatch):
    user = user.copy(update={"is_active": False})
    monkeypatch.setattr(iris.api.security, "get_user", fake_get_user(user))
    async with api_client_factory(override_user=None) as c:
        response = await c.get("/profile")
        assert response.status_code == 401


# # --- GET /profile ---


@pytest.mark.asyncio
async def test_get_profile(api_client_factory, user):
    async with api_client_factory(override_user=user) as c:
        response = await c.get("/profile")
        assert Profile(**response.json()) == user
