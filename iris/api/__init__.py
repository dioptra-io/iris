"""API initialization."""

from fastapi import APIRouter

from iris.api import agents, measurements, targets, users

# Register API routes
router = APIRouter()

router.include_router(users.router)
router.include_router(agents.router, prefix="/agents", tags=["Agents"])
router.include_router(targets.router, prefix="/targets", tags=["Targets"])
router.include_router(
    measurements.router, prefix="/measurements", tags=["Measurements"]
)
