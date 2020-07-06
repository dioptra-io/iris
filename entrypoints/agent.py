"""Agent entrypoint."""

import asyncio

from iris.agent.main import main


def app():
    """ASGI interface."""
    asyncio.run(main())


if __name__ == "__main__":
    app()
