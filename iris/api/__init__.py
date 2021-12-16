"""API initialization."""

from fastapi import APIRouter

from iris.api import agents, measurements, public, targets, users

# Register API routes
router = APIRouter()

router.include_router(users.router)
router.include_router(agents.router, prefix="/agents", tags=["Agents"])
router.include_router(targets.router, prefix="/targets", tags=["Targets"])
router.include_router(
    measurements.router, prefix="/measurements", tags=["Measurements"]
)
router.include_router(
    public.router, prefix="/measurements", tags=["Public Measurements"]
)
