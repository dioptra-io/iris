from uuid import uuid4

from tests.assertions import assert_status_code


async def test_maintenance_dramatiq(make_client, make_user, redis):
    user = make_user(is_superuser=True)
    client = make_client(user)
    queue = "test-queue"

    response = client.post(
        f"/maintenance/dq/{queue}/messages",
        json=dict(
            actor="watch_measurement_agent",
            kwargs=dict(measurement_uuid=str(uuid4()), agent_uuid=str(uuid4())),
        ),
    )
    assert_status_code(response, 201)

    response = client.get(f"/maintenance/dq/{queue}/messages")
    messages = response.json()
    assert len(messages) == 1

    redis_message_id = messages[0]["options"]["redis_message_id"]
    response = client.delete(f"/maintenance/dq/{queue}/messages/{redis_message_id}")
    assert_status_code(response, 204)

    response = client.get(f"/maintenance/dq/{queue}/messages")
    messages = response.json()
    assert len(messages) == 0
