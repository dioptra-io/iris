"""API initialization."""

from fastapi import APIRouter

from iris.api import agents, measurements, profile, results, targets

# Register API routes
router = APIRouter()
router.include_router(profile.router, prefix="/profile", tags=["Profile"])
router.include_router(agents.router, prefix="/agents", tags=["Agents"])
router.include_router(targets.router, prefix="/targets", tags=["Targets"])
router.include_router(
    measurements.router, prefix="/measurements", tags=["Measurements"]
)
router.include_router(results.router, prefix="/results", tags=["Results"])
