"""Users operations."""

import databases
from fastapi import APIRouter, Depends, Query, Request
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

github_oauth_client = GitHubOAuth2(
    settings.API_OAUTH_GITHUB_CLIENT_ID, settings.API_OAUTH_GITHUB_CLIENT_SECRET
)
router.include_router(
    fastapi_users.get_oauth_router(github_oauth_client, settings.API_TOKEN_SECRET_KEY),
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
    filter_verified: bool = False,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: public.UserDB = Depends(current_superuser),
    sqlalchemy: databases.Database = Depends(get_sqlalchemy),
):
    """Get all users."""
    where_clause = "WHERE is_verified = false" if filter_verified else ""
    users = await sqlalchemy.fetch_all(f"SELECT * FROM user {where_clause}")
    users = [public.User(**dict(user)) for user in users]
    querier = ListPagination(users, request, offset, limit)
    return await querier.query()
