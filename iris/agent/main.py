import asyncio
import logging
import socket
import time

import aioredis

from iris import __version__
from iris.agent.measurements import do_measurement
from iris.agent.settings import AgentSettings
from iris.agent.ttl import find_exit_ttl_with_mtr
from iris.commons.dependencies import get_redis_context
from iris.commons.logger import Adapter, base_logger
from iris.commons.models import AgentParameters, AgentState, MeasurementRoundRequest
from iris.commons.redis import Redis
from iris.commons.storage import Storage
from iris.commons.utils import cancel_task, get_ipv4_address, get_ipv6_address


async def heartbeat(agent_uuid: str, redis: Redis) -> None:
    """Periodically register the agent."""
    while True:
        await redis.register_agent(agent_uuid, 30)
        await asyncio.sleep(5)


async def consumer(
    redis: Redis,
    storage: Storage,
    settings: AgentSettings,
    queue: asyncio.Queue,
) -> None:
    """Consume tasks from the queue and run measurements."""
    while True:
        request: MeasurementRoundRequest = await queue.get()
        logger = Adapter(
            base_logger,
            dict(
                component="agent",
                measurement_uuid=request.measurement_agent.measurement_uuid,
                agent_uuid=request.measurement_agent.agent_uuid,
            ),
        )
        await redis.set_agent_state(settings.AGENT_UUID, AgentState.Working)
        await do_measurement(settings, request, logger, redis, storage)
        await redis.set_agent_state(settings.AGENT_UUID, AgentState.Idle)


async def main(settings=AgentSettings()):
    """Main agent function."""
    # TODO: Do not set logging level is run from tests (make two separate main functions?)
    logging.basicConfig(level=settings.STREAM_LOGGING_LEVEL)
    logger = Adapter(
        base_logger, dict(component="agent", agent_uuid=settings.AGENT_UUID)
    )
    storage = Storage(settings, logger)
    async with get_redis_context(settings, logger) as redis:
        await main_with_deps(logger, redis, settings, storage)


async def main_with_deps(
    logger: Adapter, redis: Redis, settings: AgentSettings, storage: Storage
):
    settings.AGENT_RESULTS_DIR_PATH.mkdir(parents=True, exist_ok=True)
    settings.AGENT_TARGETS_DIR_PATH.mkdir(parents=True, exist_ok=True)

    if settings.AGENT_MIN_TTL < 0:
        settings.AGENT_MIN_TTL = find_exit_ttl_with_mtr(
            settings.AGENT_MIN_TTL_FIND_TARGET, min_ttl=2, logger=logger
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
        await redis.set_agent_state(settings.AGENT_UUID, AgentState.Idle)
        await redis.set_agent_parameters(
            settings.AGENT_UUID,
            AgentParameters(
                version=__version__,
                hostname=socket.gethostname(),
                ipv4_address=get_ipv4_address(),
                ipv6_address=get_ipv6_address(),
                min_ttl=settings.AGENT_MIN_TTL,
                max_probing_rate=settings.AGENT_MAX_PROBING_RATE,
                tags=settings.AGENT_TAGS,
            ),
        )

        queue = asyncio.Queue()
        tasks = [
            asyncio.create_task(heartbeat(settings.AGENT_UUID, redis)),
            asyncio.create_task(consumer(redis, storage, settings, queue)),
            asyncio.create_task(redis.subscribe(settings.AGENT_UUID, queue)),
        ]
        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        pass

    except Exception as e:
        logger.exception(e)
        raise e

    finally:
        for task in tasks:
            await cancel_task(task)
        await redis.delete_agent_state(settings.AGENT_UUID)
        await redis.delete_agent_parameters(settings.AGENT_UUID)
        await redis.unregister_agent(settings.AGENT_UUID)
