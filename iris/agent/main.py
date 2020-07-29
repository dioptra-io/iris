import asyncio
import socket

from aioredis.errors import ConnectionClosedError
from iris import __version__
from iris.agent import logger
from iris.agent.measurements import measuremement
from iris.commons.redis import AgentRedis
from iris.commons.utils import get_own_ip_address
from iris.agent.settings import AgentSettings
from uuid import uuid4


settings = AgentSettings()


async def robust_redis(redis, redis_future):
    """Perform a Redis query robust to Redis failure."""
    result = None
    try:
        result = await redis_future
    except ConnectionClosedError as error:
        logger.error(error)
        await asyncio.sleep(settings.AGENT_RECOVER_TIME_REDIS_FAILURE)
        try:
            await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)
        except OSError:
            pass
    return result


async def consumer(agent_uuid, queue):
    """Wait for a task in a queue and process it."""
    redis = AgentRedis(agent_uuid)
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD, register=False)
    while True:
        measurement_uuid, measuremement = await queue.get()

        logger.info(f"Set agent `{agent_uuid}` state to `working`")
        await robust_redis(redis, redis.set_agent_state("working"))

        logger.info(f"Set measurement `{measurement_uuid}` state to `ongoing`")
        await redis.set_measurement_state(measurement_uuid, "ongoing")
        await measuremement

        logger.info(f"Set agent `{agent_uuid}` state to `idle`")
        await robust_redis(redis, redis.set_agent_state("idle"))

    await redis.close()


async def producer(redis, queue):
    """Wait a task and put in on the queue."""
    while True:
        logger.info("Wait for a new request...")
        parameters = await robust_redis(redis, redis.subscribe())
        if parameters is None:
            continue

        logger.info("New request received! Putting in task queue")
        await queue.put(
            (parameters["measurement_uuid"], measuremement(redis.uuid, parameters))
        )


async def main():
    """Main agent function."""
    agent_uuid = str(uuid4())
    redis = AgentRedis(agent_uuid)

    await asyncio.sleep(settings.AGENT_WAIT_FOR_START)
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    logger.info(f"Connected to Redis with UUID `{agent_uuid}`")

    try:

        await redis.set_agent_state("idle")
        await redis.set_agent_parameters(
            {
                "version": __version__,
                "hostname": socket.gethostname(),
                "ip_address": get_own_ip_address(),
                "probing_rate": settings.AGENT_PROBING_RATE,
                "buffer_sniffer_size": settings.AGENT_BUFFER_SNIFFER_SIZE,
                "inf_born": settings.AGENT_INF_BORN,
                "sup_born": settings.AGENT_SUP_BORN,
                "ips_per_subnet": settings.AGENT_IPS_PER_SUBNET,
                "pfring": settings.AGENT_PFRING,
            }
        )

        queue = asyncio.Queue()
        await asyncio.gather(producer(redis, queue), consumer(redis.uuid, queue))

    finally:
        for task in asyncio.all_tasks():
            task.cancel()
        await redis.delete_agent_state()
        await redis.delete_agent_parameters()
        await redis.close()


def app():
    """ASGI interface."""
    asyncio.run(main())


if __name__ == "__main__":
    app()
