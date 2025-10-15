import json
from datetime import datetime
from uuid import UUID, uuid4

from fastapi import APIRouter, Body, Depends, Response, status
from sqlmodel import Session

from iris.api.authentication import current_superuser
from iris.api.measurements import assert_measurement_visibility, cancel_measurement
from iris.api.settings import APISettings
from iris.commons.clickhouse import ClickHouse
from iris.commons.dependencies import (
    get_clickhouse,
    get_redis,
    get_session,
    get_settings,
    get_storage,
)
from iris.commons.models import Measurement, User
from iris.commons.redis import Redis
from iris.commons.storage import Storage, targets_key

router = APIRouter()


def format_message(message):
    message = message.copy()
    message["message_timestamp"] = datetime.utcfromtimestamp(
        message["message_timestamp"] / 1e3
    )
    return message


def redis_list_key(namespace, queue_name):
    return f"{namespace}:{queue_name}"


def redis_hash_key(namespace, queue_name):
    return f"{namespace}:{queue_name}.msgs"

@router.get("/agents/queue/{agent_uuid}")
async def get_agent_queue(agent_uuid: str, redis: Redis = Depends(get_redis)):
    keys = await redis.get_requests(agent_uuid)
    return keys

@router.delete("/agents/queue/{agent_uuid}/{measurement_uuid}")
async def delete_agent_queue_request(agent_uuid: str, measurement_uuid:str, redis: Redis = Depends(get_redis)):
    keys = await redis.get_requests(agent_uuid)
    round_requests = await redis.get_requests(agent_uuid)
    if measurement_uuid in round_requests:
       await redis.delete_request(measurement_uuid, agent_uuid)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@router.get("/dq/{queue}/messages")
async def get_dramatiq_messages(queue: str, redis: Redis = Depends(get_redis)):
    obj = await redis.client.hgetall(redis_hash_key(redis.ns, queue))
    messages = [format_message(json.loads(msg)) for msg in obj.values()]
    return sorted(messages, key=lambda x: x["message_timestamp"], reverse=True)


@router.post("/dq/{queue}/messages", status_code=status.HTTP_201_CREATED)
async def post_dramatiq_message(
    queue: str,
    actor: str = "watch_measurement_agent",
    kwargs: dict = Body(
        ...,
        examples=[{"measurement_uuid": "", "agent_uuid": ""}],
    ),
    redis: Redis = Depends(get_redis),
):
    # https://dramatiq.io/advanced.html#enqueueing-messages-from-other-languages
    redis_message_id = str(uuid4())
    message_id = str(uuid4())
    message_timestamp = int(datetime.utcnow().timestamp() * 1e3)
    message = dict(
        queue_name=queue,
        actor_name=actor,
        args=[],
        kwargs=kwargs,
        options=dict(redis_message_id=redis_message_id),
        message_id=message_id,
        message_timestamp=message_timestamp,
    )
    await redis.client.hset(
        redis_hash_key(redis.ns, queue), redis_message_id, json.dumps(message)
    )
    await redis.client.rpush(redis_list_key(redis.ns, queue), redis_message_id)
    return format_message(message)


@router.delete(
    "/dq/{queue}/messages/{redis_message_id}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_dramatiq_message(
    queue: str, redis_message_id: str, redis: Redis = Depends(get_redis)
):
    await redis.client.lrem(redis_list_key(redis.ns, queue), 0, redis_message_id)
    await redis.client.hdel(redis_hash_key(redis.ns, queue), redis_message_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    "/measurements/{measurement_uuid}", status_code=status.HTTP_204_NO_CONTENT
)
async def delete_measurement(
    measurement_uuid: UUID,
    clickhouse: ClickHouse = Depends(get_clickhouse),
    user: User = Depends(current_superuser),
    redis: Redis = Depends(get_redis),
    session: Session = Depends(get_session),
    settings: APISettings = Depends(get_settings),
    storage: Storage = Depends(get_storage),
):
    measurement = Measurement.get(session, str(measurement_uuid))
    assert_measurement_visibility(measurement, user, settings)
    # (1) Ensure that the measurement is not running anymore
    await cancel_measurement(
        measurement_uuid=measurement_uuid,
        user=user,
        redis=redis,
        session=session,
        settings=settings,
    )
    for agent in measurement.agents:
        # (2) Delete ClickHouse tables
        await clickhouse.drop_tables(agent.measurement_uuid, agent.agent_uuid)
        # (3) Delete archived target lists
        await storage.delete_file_no_check(
            storage.archive_bucket(str(user.id)),
            targets_key(agent.measurement_uuid, agent.agent_uuid),
        )
        # (4) Delete measurement metadata
        session.delete(agent)
    session.delete(measurement)
    session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
