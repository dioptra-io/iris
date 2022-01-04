import asyncio
import socket
import time
import traceback
from logging import LoggerAdapter

import aioredis

from iris import __version__
from iris.agent.measurements import do_measurement
from iris.agent.settings import AgentSettings
from iris.agent.ttl import find_exit_ttl_with_mtr
from iris.commons.logger import Adapter, base_logger
from iris.commons.models.agent import AgentParameters, AgentState
from iris.commons.models.measurement_agent import MeasurementAgentState
from iris.commons.models.measurement_round_request import MeasurementRoundRequest
from iris.commons.redis import AgentRedis
from iris.commons.storage import Storage
from iris.commons.utils import get_ipv4_address, get_ipv6_address


async def heartbeat(redis: AgentRedis) -> None:
    """Periodically register the agent."""
    while True:
        await redis.register(30)
        await asyncio.sleep(5)


async def producer(
    redis: AgentRedis, queue: asyncio.Queue, logger: LoggerAdapter
) -> None:
    """Consume tasks from a Redis channel and put them on a queue."""
    while True:
        logger.info("Waiting for requests...")
        logger.info("Measurements in queue: %s", queue.qsize())
        request = await redis.subscribe()
        await queue.put(request)


async def consumer(
    redis: AgentRedis,
    storage: Storage,
    settings: AgentSettings,
    queue: asyncio.Queue,
) -> None:
    """Consume tasks from the queue and run measurements."""
    while True:
        request: MeasurementRoundRequest = await queue.get()
        measurement_agent = request.measurement_agent
        measurement_agent.measurement = request.measurement
        logger = Adapter(
            base_logger,
            dict(
                component="agent",
                measurement_uuid=measurement_agent.measurement,
                agent_uuid=measurement_agent.agent_uuid,
            ),
        )

        measurement_state = await redis.get_measurement_state(
            measurement_agent.measurement_uuid
        )
        if measurement_state in [
            MeasurementAgentState.Canceled,
            MeasurementAgentState.Unknown,
        ]:
            logger.warning("The measurement has been canceled")
            continue

        logger.info("Set agent state to `working`")
        await redis.set_agent_state(AgentState.Working)

        logger.info("Set measurement state to `ongoing`")
        await redis.set_measurement_state(
            measurement_agent.measurement_uuid, MeasurementAgentState.Ongoing
        )

        logger.info("Launch measurement procedure")
        await do_measurement(settings, request, logger, redis, storage)

        logger.info("Set agent state to `idle`")
        await redis.set_agent_state(AgentState.Idle)


async def main(settings=AgentSettings()):
    """Main agent function."""
    logger = Adapter(
        base_logger, dict(component="agent", agent_uuid=settings.AGENT_UUID)
    )
    redis = AgentRedis(
        await settings.redis_client(), settings, logger, settings.AGENT_UUID
    )
    storage = Storage(settings, logger)

    settings.AGENT_RESULTS_DIR_PATH.mkdir(parents=True, exist_ok=True)
    settings.AGENT_TARGETS_DIR_PATH.mkdir(parents=True, exist_ok=True)

    if settings.AGENT_MIN_TTL < 0:
        settings.AGENT_MIN_TTL = find_exit_ttl_with_mtr(
            str(settings.AGENT_MIN_TTL_FIND_TARGET), min_ttl=2
        )

    while True:
        try:
            await redis.client.ping()
            break
        except aioredis.exceptions.ConnectionError:
            logger.info("Waiting for redis...")
            time.sleep(1)

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
                tags=settings.AGENT_TAGS,
            )
        )

        queue = asyncio.Queue()
        tasks = [
            asyncio.create_task(heartbeat(redis)),
            asyncio.create_task(producer(redis, queue, logger)),
            asyncio.create_task(consumer(redis, storage, settings, queue)),
        ]
        await asyncio.gather(*tasks)

    except Exception as exception:
        traceback_content = traceback.format_exc()
        for line in traceback_content.splitlines():
            logger.critical(line)
        raise exception

    finally:
        for task in tasks:
            task.cancel()
        await redis.delete_agent_state()
        await redis.delete_agent_parameters()
        await redis.deregister()
        await redis.disconnect()
