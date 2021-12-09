import databases
from fastapi_users.db import SQLAlchemyBaseUserTable, SQLAlchemyUserDatabase
from sqlalchemy import Column
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.types import Boolean, Integer

from iris.api.settings import APISettings
from iris.commons.database import Database
from iris.commons.logger import create_logger
from iris.commons.redis import Redis
from iris.commons.schemas.public import UserDB
from iris.commons.storage import Storage

settings = APISettings()
logger = create_logger(settings)


Base: DeclarativeMeta = declarative_base()


class UserTable(Base, SQLAlchemyBaseUserTable):
    probing_enabled = Column(Boolean, nullable=False, default=False)
    probing_limit = Column(Integer, nullable=False, default=0)


users = UserTable.__table__


def get_database():
    return Database(settings, logger)


def get_sqlalchemy():
    return databases.Database(settings.SQLALCHEMY_DATABASE_URL)


async def get_redis():
    client = await settings.redis_client()
    try:
        yield Redis(client, settings, logger)
    finally:
        await client.close()


def get_session():
    yield SQLAlchemyUserDatabase(UserDB, get_sqlalchemy(), users)


def get_storage():
    return Storage(settings, logger)
