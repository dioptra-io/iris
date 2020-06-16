import asyncio

from diamond_miner.agent import logger
from diamond_miner.agent.measurements import measuremement
from diamond_miner.commons.redis import Redis


async def main():
    """Main agent loop."""
    redis = Redis()
    await redis.connect("redis://redis")
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
