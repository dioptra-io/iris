from iris.commons.models.pagination import Paginated
from iris.commons.models.user import UserRead
from tests.assertions import assert_response, assert_status_code, cast_response
from tests.helpers import register_user, verify_user


async def test_get_users_not_superuser(make_client, make_user):
    client = make_client(make_user())
    assert_status_code(client.get("/users"), 401)


async def test_get_users_empty(make_client, make_user):
    client = make_client(make_user(is_superuser=True))
    assert_response(client.get("/users"), Paginated[UserRead](count=0, results=[]))


async def test_get_users(make_client, make_user):
    client = make_client()
    register_user(client)
    register_user(client)

    client = make_client(make_user(is_superuser=True))
    results = cast_response(client.get("/users"), Paginated[UserRead])
    assert results.count == 2


async def test_get_users_verified(make_client, make_user, session):
    client = make_client()
    user1 = register_user(client)
    user2 = register_user(client)
    verify_user(session, user2.id)

    client = make_client(make_user(is_superuser=True))
    assert_response(
        client.get("/users", params=dict(filter_verified=True)),
        Paginated[UserRead](count=1, results=[user1]),
    )
