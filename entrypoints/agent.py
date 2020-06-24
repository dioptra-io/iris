"""Agent entrypoint."""

import asyncio

from diamond_miner.agent.main import main


def app():
    """ASGI interface."""
    asyncio.run(main())


if __name__ == "__main__":
    app()
