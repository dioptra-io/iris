from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import func, select
from sqlmodel import Session

from iris.api.authentication import (
    cookie_auth_backend,
    current_superuser,
    current_verified_user,
    fastapi_users,
    jwt_auth_backend,
)
from iris.api.dependencies import get_session, get_settings, get_storage
from iris.api.settings import APISettings
from iris.commons.models import ExternalServices, Paginated, User, UserDB, UserTable
from iris.commons.storage import Storage

router = APIRouter()


# Authentication routes
router.include_router(
    fastapi_users.get_auth_router(cookie_auth_backend),
    prefix="/auth/cookie",
    tags=["Authentication"],
)
router.include_router(
    fastapi_users.get_auth_router(jwt_auth_backend),
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
    _user: UserDB = Depends(current_superuser),
    session: Session = Depends(get_session),
):
    count_query = select(func.count(UserTable.id))
    user_query = select(UserTable).offset(offset).limit(limit)
    if filter_verified:
        count_query = count_query.where(UserTable.is_verified != True)  # noqa: E712
        user_query = user_query.where(UserTable.is_verified != True)  # noqa: E712
    count = session.execute(count_query).one()[0]
    users = session.execute(user_query).fetchall()
    users_db = [UserDB.from_orm(x[0]) for x in users]
    return Paginated.from_results(request.url, users_db, count, offset, limit)


@router.get(
    "/users/me/services",
    response_model=ExternalServices,
    summary="Get external services credentials",
    tags=["Users"],
)
async def get_user_services(
    settings: APISettings = Depends(get_settings),
    storage: Storage = Depends(get_storage),
    _user: UserDB = Depends(current_verified_user),
):
    s3_credentials = await storage.generate_temporary_credentials()
    return ExternalServices(
        chproxy_url=settings.CHPROXY_PUBLIC_URL,
        chproxy_username=settings.CHPROXY_PUBLIC_USERNAME,
        chproxy_password=settings.CHPROXY_PUBLIC_PASSWORD,
        s3_host=settings.S3_HOST,
        s3_access_key_expiration=s3_credentials["Expiration"],
        s3_access_key_id=s3_credentials["AccessKeyId"],
        s3_secret_access_key=s3_credentials["SecretAccessKey"],
        s3_session_token=s3_credentials["SessionToken"],
    )
