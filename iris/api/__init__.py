"""API initialization."""

from fastapi import APIRouter

from iris.api import agents, measurements, targets
from iris.api.users import fastapi_users, jwt_authentication

# Register API routes
router = APIRouter()

# Authentication routes
router.include_router(
    fastapi_users.get_auth_router(jwt_authentication),
    prefix="/auth/jwt",
    tags=["Authentication"],
)
router.include_router(
    fastapi_users.get_register_router(), prefix="/auth", tags=["Authentication"]
)
router.include_router(
    fastapi_users.get_reset_password_router(),
    prefix="/auth",
    tags=["Authentication"],
)

# Users routes
router.include_router(
    fastapi_users.get_users_router(),
    prefix="/users",
    tags=["Users"],
)

# Other routes
router.include_router(agents.router, prefix="/agents", tags=["Agents"])
router.include_router(targets.router, prefix="/targets", tags=["Targets"])
router.include_router(
    measurements.router, prefix="/measurements", tags=["Measurements"]
)
