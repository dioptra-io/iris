from fastapi import APIRouter, Depends, Query, Request
from fastapi_users_db_sqlalchemy import SQLAlchemyUserDatabase
from httpx_oauth.clients.github import GitHubOAuth2

from iris.api.authentication import (
    current_superuser,
    current_verified_user,
    fastapi_users,
    jwt_authentication,
)
from iris.api.dependencies import get_settings, get_storage, get_user_db
from iris.commons.models import ExternalServices, Paginated, User, UserDB
from iris.commons.storage import Storage

router = APIRouter()
settings = get_settings()


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
    response_model=Paginated[User],
    summary="Get all users (Admin only).",
    tags=["Users"],
)
async def get_users(
    request: Request,
    filter_verified: bool = False,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    user_db: SQLAlchemyUserDatabase = Depends(get_user_db),
    _user: UserDB = Depends(current_superuser),
):
    query = user_db.users.select()
    if filter_verified:
        query = query.where(not UserDB.is_verified)
    users = await user_db.database.fetch_all(query)
    # TODO: Proper SQL limit/offset
    #  => override SQLAlchemyUserDatabase with .all()/.count()?
    return Paginated.from_results(request.url, users, len(users), offset, limit)


@router.get(
    "/users/me/services",
    response_model=ExternalServices,
    summary="Get external services credentials",
    tags=["Users"],
)
async def get_user_services(
    storage: Storage = Depends(get_storage),
    _user: UserDB = Depends(current_verified_user),
):
    s3_credentials = await storage.generate_temporary_credentials()
    return ExternalServices(
        chproxy_url=settings.CHPROXY_PUBLIC_URL,
        chproxy_username=settings.CHPROXY_PUBLIC_USERNAME,
        chproxy_password=settings.CHPROXY_PUBLIC_PASSWORD,
        s3_host=settings.AWS_S3_HOST,
        s3_access_key_expiration=s3_credentials["Expiration"],
        s3_access_key_id=s3_credentials["AccessKeyId"],
        s3_secret_access_key=s3_credentials["SecretAccessKey"],
        s3_session_token=s3_credentials["SessionToken"],
    )
