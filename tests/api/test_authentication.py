from uuid import uuid4

import jwt
import pytest
from fastapi_users.authentication.transport.bearer import BearerResponse

from iris.commons.models.user import User
from tests.assertions import assert_status_code, cast_response

pytestmark = pytest.mark.asyncio


# TODO: Move to helpers?
async def register_user(client, **kwargs):
    default = dict(
        email=f"{uuid4()}@example.org",
        password="password",
        firstname="firstname",
        lastname="lastname",
    )
    return client.post("/auth/register", json={**default, **kwargs})


async def test_register(make_client):
    client = make_client(None)
    response = await register_user(client)
    user = cast_response(response, User)
    assert user.is_active
    assert not user.is_superuser
    assert not user.is_verified
    assert not user.probing_enabled


async def test_register_overridden_fields(make_client):
    client = make_client()
    response = await register_user(
        client,
        is_active=False,
        is_superuser=True,
        is_verified=True,
        probing_enabled=True,
    )
    user = cast_response(response, User)
    assert user.is_active
    assert not user.is_superuser
    assert not user.is_verified
    assert not user.probing_enabled


async def test_register_invalid_email(make_client):
    client = make_client()
    response = await register_user(client, email="email")
    assert_status_code(response, 422)


async def test_login(make_client, api_settings):
    client = make_client()
    email = f"{uuid4()}@example.org"
    await register_user(client, email=email)
    response = client.post(
        "/auth/jwt/login", data=dict(username=email, password="password")
    )
    token = cast_response(response, BearerResponse)
    assert token.token_type == "bearer"
    decoded = jwt.decode(
        token.access_token,
        api_settings.API_TOKEN_SECRET_KEY,
        algorithms=["HS256"],
        audience="fastapi-users:auth",
    )
    assert decoded["user_id"]
    assert decoded["is_active"]
    assert not decoded["is_verified"]
    assert not decoded["is_superuser"]
    assert not decoded["probing_enabled"]
