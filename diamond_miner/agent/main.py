import asyncio

from diamond_miner.agent import logger
from diamond_miner.agent.measurements import measuremement
from diamond_miner.commons.redis import AgentRedis
from diamond_miner.commons.utils import get_own_ip_address
from diamond_miner.agent.settings import AgentSettings
from uuid import uuid4


settings = AgentSettings()


async def consumer(agent_uuid, queue):
    """Wait for a task in a queue and process it."""
    redis = AgentRedis(agent_uuid)
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD, register=False)
    while True:
        measurement_uuid, measuremement = await queue.get()
        logger.info(f"Set agent `{agent_uuid}` state to `working`")
        await redis.set_agent_state("working")
        logger.info(f"Set measurement `{measurement_uuid}` state to `ongoing`")
        await redis.set_measurement_state(measurement_uuid, "ongoing")
        await measuremement
        logger.info(f"Set agent `{agent_uuid}` state to `idle`")
        await redis.set_agent_state("idle")
    await redis.close()


async def producer(redis, queue):
    """Wait a task and put in on the queue."""
    while True:
        logger.info("Wait for a new request...")
        parameters = await redis.subscribe()
        logger.info("New request received! Putting in task queue")
        await queue.put(
            (parameters["measurement_uuid"], measuremement(redis.uuid, parameters))
        )


async def main():
    """Main agent function."""
    redis = AgentRedis(str(uuid4()))

    await asyncio.sleep(settings.AGENT_WAIT_FOR_START)
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    try:

        await redis.set_agent_state("idle")
        await redis.set_agent_parameters(
            {
                "ip_address": get_own_ip_address(),
                "probing_rate": settings.AGENT_PROBING_RATE,
                "buffer_sniffer_size": settings.AGENT_BUFFER_SNIFFER_SIZE,
                "inf_born": settings.AGENT_INF_BORN,
                "sup_born": settings.AGENT_SUP_BORN,
                "ips_per_subnet": settings.AGENT_IPS_PER_SUBNET,
            }
        )

        queue = asyncio.Queue()
        await asyncio.gather(producer(redis, queue), consumer(redis.uuid, queue))

    finally:
        await redis.delete_agent_state()
        await redis.delete_agent_parameters()
        await redis.close()
