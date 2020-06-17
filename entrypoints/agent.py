import asyncio

from diamond_miner.agent import logger
from diamond_miner.agent.measurements import measuremement
from diamond_miner.commons.redis import Redis
from diamond_miner.agent.settings import AgentSettings


settings = AgentSettings()


async def main():
    """Main agent loop."""
    redis = Redis()
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)
    await redis.set(f"state:{redis.uuid}", 1)

    while True:
        logger.info("Wait for a new request...")
        parameters = await redis.subscribe("request:all")

        await measuremement(redis, parameters)

    await redis.close()


def app():
    """ASGI interface."""
    asyncio.run(main())


if __name__ == "__main__":
    app()
