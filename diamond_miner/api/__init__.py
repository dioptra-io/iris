"""API initialization."""

import logging

# Set logger
logger = logging.getLogger("api")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: API :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate = False

from fastapi import APIRouter  # noqa
from diamond_miner.api import (  # noqa
    authentication,
    configuration,
    agents,
    targets,
    measurements,
)


# Register API routes
router = APIRouter()
router.include_router(
    authentication.router, prefix="/authentication", tags=["Authentication"]
)
router.include_router(
    configuration.router, prefix="/configuration", tags=["Configuration"]
)
router.include_router(agents.router, prefix="/agents", tags=["Agents"])
router.include_router(targets.router, prefix="/targets", tags=["Targets"])
router.include_router(
    measurements.router, prefix="/measurements", tags=["Measurements"]
)


__version__ = "0.1.0"
