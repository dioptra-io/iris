from datetime import datetime

import httpx
from diamond_miner.queries import (
    links_table,
    prefixes_table,
    probes_table,
    results_table,
)
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
from iris.api.settings import APISettings
from iris.commons.dependencies import get_session, get_settings, get_storage
from iris.commons.models import ExternalServices, Measurement, Paginated, User, UserRead
from iris.commons.models.user import (
    AWSCredentials,
    ClickHouseCredentials,
    UserCreate,
    UserUpdate,
)
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
    fastapi_users.get_register_router(UserRead, UserCreate),
    prefix="/auth",
    tags=["Authentication"],
)

# Users routes
router.include_router(
    fastapi_users.get_users_router(UserRead, UserUpdate),
    prefix="/users",
    tags=["Users"],
)


@router.get(
    "/users",
    response_model=Paginated[UserRead],
    summary="Get all users (Admin only).",
    tags=["Users"],
)
async def get_users(
    request: Request,
    filter_verified: bool = False,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    _user: User = Depends(current_superuser),
    session: Session = Depends(get_session),
):
    count_query = select(func.count(User.id))
    user_query = select(User).offset(offset).limit(limit)
    if filter_verified:
        count_query = count_query.where(User.is_verified != True)  # noqa: E712
        user_query = user_query.where(User.is_verified != True)  # noqa: E712
    count = session.execute(count_query).one()[0]
    users = session.execute(user_query).fetchall()
    users = [x[0] for x in users]
    return Paginated.from_results(request.url, users, count, offset, limit)


@router.get(
    "/users/me/services",
    response_model=ExternalServices,
    summary="Get external services credentials",
    tags=["Users"],
)
async def get_user_services(
    session: Session = Depends(get_session),
    settings: APISettings = Depends(get_settings),
    storage: Storage = Depends(get_storage),
    user: User = Depends(current_verified_user),
):
    tables = []
    public_measurements = Measurement.all(session, tags=[settings.TAG_PUBLIC])
    user_measurements = Measurement.all(session, user_id=str(user.id))
    for measurement in public_measurements + user_measurements:
        tables += [
            f"{settings.CLICKHOUSE_DATABASE}.{links_table(measurement.uuid)}",
            f"{settings.CLICKHOUSE_DATABASE}.{prefixes_table(measurement.uuid)}",
            f"{settings.CLICKHOUSE_DATABASE}.{probes_table(measurement.uuid)}",
            f"{settings.CLICKHOUSE_DATABASE}.{results_table(measurement.uuid)}",
        ]
    if user.is_superuser:
        tables += [f"{settings.CLICKHOUSE_DATABASE}.*"]
    clickhouse_credentials = {}
    if settings.GUESTHOUSE_URL:
        async with httpx.AsyncClient(base_url=settings.GUESTHOUSE_URL) as client:
            params = {
                "username": settings.CLICKHOUSE_USERNAME,
                "password": settings.CLICKHOUSE_PASSWORD,
                "lifetime": 60 * 60 * 3,
                "tables": tables,
            }
            clickhouse_credentials = (await client.post("/", json=params)).json()
    s3_credentials = await storage.generate_temporary_credentials()
    return ExternalServices(
        clickhouse=ClickHouseCredentials(
            base_url=clickhouse_credentials.get("base_url", ""),
            database=settings.CLICKHOUSE_DATABASE,
            username=clickhouse_credentials.get("username", ""),
            password=clickhouse_credentials.get("password", ""),
        ),
        clickhouse_expiration_time=clickhouse_credentials.get(
            "expiration_time", datetime.now()
        ),
        s3=AWSCredentials(
            aws_access_key_id=s3_credentials["AccessKeyId"],
            aws_secret_access_key=s3_credentials["SecretAccessKey"],
            aws_session_token=s3_credentials["SessionToken"],
            endpoint_url=settings.S3_ENDPOINT_URL,
        ),
        s3_expiration_time=s3_credentials["Expiration"],
    )
