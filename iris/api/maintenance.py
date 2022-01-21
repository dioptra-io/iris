import json
from datetime import datetime
from uuid import uuid4

from fastapi import APIRouter, Body, Depends

from iris.commons.dependencies import get_redis
from iris.commons.redis import Redis

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


@router.get("/dq/{queue}/messages")
async def get_dramatiq_messages(queue: str, redis: Redis = Depends(get_redis)):
    obj = await redis.client.hgetall(redis_hash_key(redis.ns, queue))
    messages = [format_message(json.loads(msg)) for msg in obj.values()]
    return sorted(messages, key=lambda x: x["message_timestamp"], reverse=True)


@router.post("/dq/{queue}/messages")
async def post_dramatiq_message(
    queue: str,
    actor: str = "watch_measurement_agent",
    kwargs: dict = Body(
        ...,
        example={"measurement_uuid": "", "agent_uuid": ""},
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


@router.delete("/dq/{queue}/messages/{redis_message_id}")
async def delete_dramatiq_message(
    queue: str, redis_message_id: str, redis: Redis = Depends(get_redis)
):
    await redis.client.lrem(redis_list_key(redis.ns, queue), 0, redis_message_id)
    await redis.client.hdel(redis_hash_key(redis.ns, queue), redis_message_id)
