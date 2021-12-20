import databases
from fastapi_users.db import (
    SQLAlchemyBaseOAuthAccountTable,
    SQLAlchemyBaseUserTable,
    SQLAlchemyUserDatabase,
)
from sqlalchemy import Column
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base
from sqlalchemy.types import Boolean, Integer, String
from sqlmodel import Session

from iris.api.settings import APISettings
from iris.commons.database import Database
from iris.commons.logger import create_logger
from iris.commons.redis import Redis
from iris.commons.schemas.users import UserDB
from iris.commons.storage import Storage

settings = APISettings()
logger = create_logger(settings)


Base: DeclarativeMeta = declarative_base()


class UserTable(Base, SQLAlchemyBaseUserTable):
    firstname: str = Column(String, nullable=False)
    lastname: str = Column(String, nullable=False)
    probing_enabled = Column(Boolean, nullable=False, default=False)
    probing_limit = Column(Integer, nullable=True, default=0)


class OAuthAccount(SQLAlchemyBaseOAuthAccountTable, Base):
    pass


users = UserTable.__table__
oauth_accounts = OAuthAccount.__table__


def get_database():
    return Database(settings, logger)


def get_engine():
    return settings.sqlalchemy_engine()


def get_sqlalchemy():
    return databases.Database(settings.SQLALCHEMY_DATABASE_URL)


async def get_redis():
    client = await settings.redis_client()
    try:
        yield Redis(client, settings, logger)
    finally:
        await client.close()


def get_session():
    yield SQLAlchemyUserDatabase(UserDB, get_sqlalchemy(), users, oauth_accounts)


def get_sqlmodel_session():
    with Session(settings.sqlalchemy_engine()) as session:
        yield session


def get_storage():
    return Storage(settings, logger)
