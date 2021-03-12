import asyncio
import socket
import traceback

from iris import __version__
from iris.agent.measurements import measuremement
from iris.agent.settings import AgentSettings
from iris.commons.logger import create_logger
from iris.commons.redis import AgentRedis
from iris.commons.utils import get_own_ip_address


async def consumer(settings, agent_uuid, queue, logger):
    """Wait for a task in a queue and process it."""
    redis = AgentRedis(agent_uuid, settings=settings, logger=logger)
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD, register=False)

    while True:
        parameters = await queue.get()
        measurement_uuid = parameters["measurement_uuid"]

        logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"

        is_alive = await redis.test()
        if not is_alive:
            logger.error(f"{logger_prefix} Redis connection failed. Re-connecting...")
            await asyncio.sleep(settings.AGENT_RECOVER_TIME_REDIS_FAILURE)
            try:
                await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)
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
        await measuremement(settings, redis, parameters, logger)

        logger.info(f"{logger_prefix} Set agent state to `idle`")
        await redis.set_agent_state("idle")


async def producer(redis, queue, logger):
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
    settings = AgentSettings()
    logger = create_logger(settings, tags={"agent_uuid": settings.AGENT_UUID})

    redis = AgentRedis(settings.AGENT_UUID, settings=settings, logger=logger)

    await asyncio.sleep(settings.AGENT_WAIT_FOR_START)
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    logger.info(f"{settings.AGENT_UUID} :: Connected to Redis")
    tasks = []
    try:

        await redis.set_agent_state("idle")
        await redis.set_agent_parameters(
            {
                "version": __version__,
                "hostname": socket.gethostname(),
                "ip_address": get_own_ip_address(),
                "probing_rate": settings.AGENT_PROBING_RATE,
            }
        )

        queue = asyncio.Queue()
        tasks = [
            asyncio.create_task(producer(redis, queue, logger)),
            asyncio.create_task(consumer(settings, redis.uuid, queue, logger)),
        ]
        await asyncio.gather(*tasks)

    except Exception as exception:
        traceback_content = traceback.format_exc()
        for line in traceback_content.splitlines():
            logger.critical(f"{settings.AGENT_UUID} :: {line}")
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
