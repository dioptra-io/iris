import asyncio
import socket
import traceback
from logging import Logger

from iris import __version__
from iris.agent.measurements import measurement
from iris.agent.settings import AgentSettings
from iris.agent.ttl import find_exit_ttl
from iris.commons.logger import create_logger
from iris.commons.redis import AgentRedis
from iris.commons.schemas.public import AgentParameters, AgentState, MeasurementState
from iris.commons.storage import Storage
from iris.commons.utils import get_ipv4_address, get_ipv6_address


async def heartbeat(redis: AgentRedis) -> None:
    """Periodically register the agent."""
    while True:
        await redis.register(15)
        await asyncio.sleep(5)


async def producer(redis: AgentRedis, queue: asyncio.Queue, logger: Logger) -> None:
    """Consume tasks from a Redis channel and put them on a queue."""
    while True:
        logger.info(f"{redis.uuid} :: Waiting for requests...")
        logger.info(f"{redis.uuid} :: Measurements in queue: {queue.qsize()}")
        request = await redis.subscribe()
        await queue.put(request)


async def consumer(
    redis: AgentRedis,
    storage: Storage,
    settings: AgentSettings,
    queue: asyncio.Queue,
    logger: Logger,
) -> None:
    """Consume tasks from the queue and run measurements."""
    while True:
        request = await queue.get()
        logger_prefix = f"{request.measurement.uuid} :: {redis.uuid} ::"

        measurement_state = await redis.get_measurement_state(request.measurement.uuid)
        if measurement_state in [
            MeasurementState.Canceled,
            MeasurementState.Unknown,
        ]:
            logger.warning(f"{logger_prefix} The measurement has been canceled")
            continue

        logger.info(f"{logger_prefix} Set agent state to `working`")
        await redis.set_agent_state(AgentState.Working)

        logger.info(f"{logger_prefix} Set measurement state to `ongoing`")
        await redis.set_measurement_state(
            request.measurement.uuid, MeasurementState.Ongoing
        )

        logger.info(f"{logger_prefix} Launch measurement procedure")
        await measurement(settings, request, logger, redis, storage)

        logger.info(f"{logger_prefix} Set agent state to `idle`")
        await redis.set_agent_state(AgentState.Idle)


async def main():
    """Main agent function."""
    settings = AgentSettings()
    logger = create_logger(settings)
    redis = AgentRedis(
        await settings.redis_client(), settings, logger, settings.AGENT_UUID
    )
    storage = Storage(settings, logger)

    if settings.AGENT_MIN_TTL < 0:
        settings.AGENT_MIN_TTL = find_exit_ttl(
            logger, settings.AGENT_MIN_TTL_FIND_TARGET, min_ttl=2
        )

    await asyncio.sleep(settings.AGENT_WAIT_FOR_START)

    tasks = []
    try:
        await redis.set_agent_state(AgentState.Idle)
        await redis.set_agent_parameters(
            AgentParameters(
                version=__version__,
                hostname=socket.gethostname(),
                ipv4_address=get_ipv4_address(),
                ipv6_address=get_ipv6_address(),
                min_ttl=settings.AGENT_MIN_TTL,
                max_probing_rate=settings.AGENT_MAX_PROBING_RATE,
                agent_tags=settings.AGENT_TAGS,
            )
        )

        queue = asyncio.Queue()
        tasks = [
            asyncio.create_task(heartbeat(redis)),
            asyncio.create_task(producer(redis, queue, logger)),
            asyncio.create_task(consumer(redis, storage, settings, queue, logger)),
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
        await redis.delete_agent_state()
        await redis.delete_agent_parameters()
        await redis.deregister()
        await redis.disconnect()


def app():
    asyncio.run(main())


if __name__ == "__main__":
    app()
