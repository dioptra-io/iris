import asyncio

from diamond_miner.agent import logger
from diamond_miner.agent.measurements import measuremement
from diamond_miner.commons.redis import Redis
from diamond_miner.agent.settings import AgentSettings
from uuid import uuid4


settings = AgentSettings()


async def consumer(uuid, queue):
    """Wait for a task in a queue and process it."""
    redis = Redis()
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)
    while True:
        measurement_uuid, task = await queue.get()
        logger.info("Request consumption")
        await redis.set(f"state:{uuid}", 0)
        await redis.set(f"published:{measurement_uuid}", "ongoing")
        await task
        await redis.set(f"state:{uuid}", 1)
    await redis.close()


async def producer(redis, queue):
    """Wait a task and put in on the queue."""
    while True:
        logger.info("Wait for a new request...")
        parameters = await redis.subscribe("request:all", f"request:{redis.uuid}")
        logger.info("New request received! Putting in task queue")
        await queue.put(
            (parameters["measurement_uuid"], measuremement(redis.uuid, parameters))
        )


async def main():
    """Main agent function."""
    redis = Redis(uuid=str(uuid4()))

    await asyncio.sleep(5)
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)
    await redis.set(f"state:{redis.uuid}", 1)

    queue = asyncio.Queue()
    await asyncio.gather(producer(redis, queue), consumer(redis.uuid, queue))

    await redis.delete(f"state:{redis.uuid}")
    await redis.close()


def app():
    """ASGI interface."""
    asyncio.run(main())


if __name__ == "__main__":
    app()
