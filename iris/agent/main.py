import asyncio
import socket
import ssl
import traceback

from iris import __version__
from iris.agent import logger
from iris.agent.measurements import measuremement
from iris.commons.redis import AgentRedis
from iris.commons.utils import get_own_ip_address
from iris.agent.settings import AgentSettings
from uuid import uuid4


settings = AgentSettings()
settings_redis_ssl = ssl.SSLContext() if settings.REDIS_SSL else None


async def consumer(agent_uuid, queue):
    """Wait for a task in a queue and process it."""
    redis = AgentRedis(agent_uuid)
    await redis.connect(
        settings.REDIS_URL,
        settings.REDIS_PASSWORD,
        ssl=settings_redis_ssl,
        register=False,
    )
    while True:
        parameters = await queue.get()
        measurement_uuid = parameters["measurement_uuid"]

        logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"

        is_alive = await redis.test()
        if not is_alive:
            logger.error(f"{logger_prefix} Redis connection failed. Re-connecting...")
            await asyncio.sleep(settings.AGENT_RECOVER_TIME_REDIS_FAILURE)
            try:
                await redis.connect(
                    settings.REDIS_URL, settings.REDIS_PASSWORD, ssl=settings_redis_ssl
                )
            except OSError:
                continue

        measurement_state = await redis.get_measurement_state(measurement_uuid)
        if measurement_state is None:
            logger.warning(f"{logger_prefix} The measurement has been canceled")
            continue

        logger.info(f"{logger_prefix} Set agent state to `working`")
        await redis.set_agent_state("working")

        logger.info(f"{logger_prefix} Set measurement state to `ongoing`")
        await redis.set_measurement_state(measurement_uuid, "ongoing")

        logger.info(f"{logger_prefix} Launch measurement procedure")
        await measuremement(redis, parameters)

        logger.info(f"{logger_prefix} Set agent state to `idle`")
        await redis.set_agent_state("idle")

    await redis.disconnect()


async def producer(redis, queue):
    """Wait a task and put in on the queue."""
    while True:
        logger.info(f"{redis.uuid} :: Wait for a new request...")
        parameters = await redis.subscribe()
        if parameters is None:
            continue

        logger.info(f"{redis.uuid} :: New request received! Putting in task queue")
        await queue.put(parameters)


async def main():
    """Main agent function."""
    agent_uuid = str(uuid4()) if settings.AGENT_UUID is None else settings.AGENT_UUID
    redis = AgentRedis(agent_uuid)

    await asyncio.sleep(settings.AGENT_WAIT_FOR_START)
    await redis.connect(
        settings.REDIS_URL, settings.REDIS_PASSWORD, ssl=settings_redis_ssl
    )

    logger.info(f"{agent_uuid} :: Connected to Redis")

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
        tasks = [
            asyncio.create_task(producer(redis, queue)),
            asyncio.create_task(consumer(redis.uuid, queue)),
        ]
        await asyncio.gather(*tasks)

    except Exception as exception:
        traceback_content = traceback.format_exc()
        for line in traceback_content.splitlines():
            logger.critical(f"{agent_uuid} :: {line}")
        raise exception

    finally:
        for task in tasks:
            task.cancel()
        await redis.unsubscribe()
        await redis.delete_agent_state()
        await redis.delete_agent_parameters()
        await redis.disconnect()


def app():
    """ASGI interface."""
    asyncio.run(main())


if __name__ == "__main__":
    app()
