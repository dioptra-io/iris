"""Users operations."""

from fastapi import APIRouter

from iris.api.authentication import fastapi_users, jwt_authentication

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

# Users routes
router.include_router(
    fastapi_users.get_users_router(),
    prefix="/users",
    tags=["Users"],
)
