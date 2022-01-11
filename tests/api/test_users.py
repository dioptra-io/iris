import pytest

from iris.commons.models.pagination import Paginated
from iris.commons.models.user import ExternalServices, User
from tests.assertions import assert_response, assert_status_code, cast_response
from tests.helpers import register_user

pytestmark = pytest.mark.asyncio


async def test_get_users_not_superuser(make_client, make_user):
    client = make_client(make_user())
    assert_status_code(client.get("/users"), 401)


async def test_get_users_empty(make_client, make_user):
    client = make_client(make_user(is_superuser=True))
    assert_response(client.get("/users"), Paginated[User](count=0, results=[]))


async def test_get_users(make_client, make_user):
    client = make_client()
    await register_user(client)
    await register_user(client)

    client = make_client(make_user(is_superuser=True))
    results = cast_response(client.get("/users"), Paginated[User])
    assert results.count == 2


async def test_get_user_services(make_client, make_user):
    client = make_client(make_user())
    assert cast_response(client.get("/users/me/services"), ExternalServices)
