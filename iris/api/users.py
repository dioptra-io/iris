"""Users operations."""

from uuid import UUID

import databases
from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from httpx_oauth.clients.github import GitHubOAuth2

from iris.api.authentication import current_superuser, fastapi_users, jwt_authentication
from iris.api.dependencies import get_sqlalchemy, settings
from iris.api.pagination import ListPagination
from iris.commons.schemas import public

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

github_oauth_client = GitHubOAuth2("CLIENT_ID", "CLIENT_SECRET")
router.include_router(
    fastapi_users.get_oauth_router(github_oauth_client, "SECRET"),
    prefix="/auth/github",
    tags=["Authentication"],
)

# Users routes
router.include_router(
    fastapi_users.get_users_router(),
    prefix="/users",
    tags=["Users"],
)


@router.get(
    "/users",
    response_model=public.Paginated[public.User],
    summary="Get all users (Admin only).",
    tags=["Users"],
)
async def get_users(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: public.UserDB = Depends(current_superuser),
    sqlalchemy: databases.Database = Depends(get_sqlalchemy),
):
    """Get all users."""
    users = await sqlalchemy.fetch_all("SELECT * FROM user")
    users = [public.User(**dict(user)) for user in users]
    querier = ListPagination(users, request, offset, limit)
    return await querier.query()


@router.patch(
    "/users/verify/{id}",
    response_model=public.User,
    summary="Verify a user (Admin only).",
    tags=["Users"],
)
async def verify_user(
    request: Request,
    id: UUID,
    user: public.UserDB = Depends(current_superuser),
    sqlalchemy: databases.Database = Depends(get_sqlalchemy),
):
    """Verify a user specified by UUID."""
    await sqlalchemy.execute(
        "UPDATE user SET is_verified = true WHERE id = :id", values={"id": str(id)}
    )
    user = await sqlalchemy.fetch_one(
        "SELECT * FROM user WHERE id = :id", values={"id": str(id)}
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return dict(user)


@router.patch(
    "/users/enable/{id}",
    response_model=public.User,
    summary="Enable probing capabilities for a user (Admin only).",
    tags=["Users"],
)
async def enable_probing_user(
    request: Request,
    id: UUID,
    user: public.UserDB = Depends(current_superuser),
    sqlalchemy: databases.Database = Depends(get_sqlalchemy),
):
    """Enable probing capabilities for a user specified by UUID."""
    await sqlalchemy.execute(
        "UPDATE user SET probing_enabled = true, probing_limit = :limit WHERE id = :id",
        values={"limit": str(settings.API_DEFAULT_PROBING_LIMIT), "id": str(id)},
    )
    user = await sqlalchemy.fetch_one(
        "SELECT * FROM user WHERE id = :id", values={"id": str(id)}
    )
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return dict(user)
