import databases
from fastapi import Depends
from fastapi_users.db import SQLAlchemyUserDatabase
from sqlmodel import Session

from iris.api.settings import APISettings
from iris.commons.logger import Adapter, base_logger
from iris.commons.models import OAuthAccount, UserDB, UserTable
from iris.commons.redis import Redis
from iris.commons.storage import Storage


def get_settings():
    return APISettings()


def get_logger():
    return Adapter(base_logger, dict(component="api"))


# TODO: Use SQLAlchemy async engine/session for both FastAPI-Users and SQLModel.
# Wait for FastAPI-Users support for SQLModel?
def get_session(settings=Depends(get_settings)):
    with Session(settings.sqlalchemy_engine()) as session:
        yield session


async def get_redis(settings=Depends(get_settings), logger=Depends(get_logger)):
    client = await settings.redis_client()
    try:
        yield Redis(client, settings, logger)
    finally:
        await client.close()


def get_user_db(settings=Depends(get_settings)):
    db = databases.Database(settings.SQLALCHEMY_DATABASE_URL)
    yield SQLAlchemyUserDatabase(
        UserDB, db, UserTable.__table__, OAuthAccount.__table__
    )


def get_storage(settings=Depends(get_settings), logger=Depends(get_logger)):
    return Storage(settings, logger)
