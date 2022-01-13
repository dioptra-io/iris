from contextlib import asynccontextmanager, contextmanager

import aioredis
from fastapi import Depends
from fastapi_users.db import SQLAlchemyUserDatabase
from fastapi_users_db_sqlalchemy.access_token import SQLAlchemyAccessTokenDatabase
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlmodel import Session

from iris.api.settings import APISettings
from iris.commons.logger import Adapter, base_logger
from iris.commons.models import UserDB, UserTable
from iris.commons.models.user import AccessToken, AccessTokenTable
from iris.commons.redis import Redis
from iris.commons.storage import Storage


def get_settings():
    return APISettings()


def get_logger():
    return Adapter(base_logger, dict(component="commons"))


def get_engine(settings=Depends(get_settings)):
    # TODO: Connection pooling.
    engine = create_engine(
        settings.DATABASE_URL,
        connect_args=dict(connect_timeout=5),
        future=True,
    )
    yield engine
    engine.dispose()


async def get_async_engine(settings=Depends(get_settings)):
    # TODO: Connection pooling.
    engine = create_async_engine(
        settings.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://"),
        connect_args=dict(command_timeout=5, timeout=5),
        future=True,
    )
    yield engine
    await engine.dispose()


# TODO: Use SQLAlchemy async engine/session for both FastAPI-Users and SQLModel.
#   => Wait for FastAPI-Users support for SQLModel?
def get_session(engine=Depends(get_engine)):
    with Session(engine) as session:
        yield session


async def get_async_session(engine=Depends(get_async_engine)):
    async with AsyncSession(engine) as session:
        yield session


def get_access_token_db(session=Depends(get_async_session)):
    return SQLAlchemyAccessTokenDatabase(AccessToken, session, AccessTokenTable)


def get_user_db(session=Depends(get_async_session)):
    return SQLAlchemyUserDatabase(UserDB, session, UserTable)


async def get_redis(settings=Depends(get_settings), logger=Depends(get_logger)):
    # TODO: Connection pooling.
    client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
    yield Redis(client, settings, logger)
    await client.close()


def get_storage(settings=Depends(get_settings), logger=Depends(get_logger)):
    return Storage(settings, logger)


get_engine_context = contextmanager(get_engine)
get_redis_context = asynccontextmanager(get_redis)
get_session_context = contextmanager(get_session)
