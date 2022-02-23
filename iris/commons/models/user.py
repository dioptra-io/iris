from datetime import datetime
from typing import Optional

from fastapi_users import models
from fastapi_users.authentication.strategy import BaseAccessToken
from fastapi_users_db_sqlalchemy import SQLAlchemyBaseUserTable
from fastapi_users_db_sqlalchemy.access_token import SQLAlchemyBaseAccessTokenTable
from pydantic import Field
from sqlalchemy import Boolean, Column, DateTime, Integer, String

from iris.commons.models.base import Base, BaseModel


class UserTable(Base, SQLAlchemyBaseUserTable):
    firstname = Column(String, nullable=False)
    lastname = Column(String, nullable=False)
    probing_enabled = Column(Boolean, nullable=False, default=False)
    probing_limit = Column(Integer, nullable=True, default=1)
    allow_tag_reserved = Column(Boolean, nullable=False, default=False)
    allow_tag_public = Column(Boolean, nullable=False, default=False)
    creation_time = Column(DateTime, nullable=False, default=datetime.utcnow)


class AccessToken(BaseAccessToken):
    pass


class AccessTokenTable(SQLAlchemyBaseAccessTokenTable, Base):
    pass


class CustomCreateUpdateDictModel(models.BaseModel):
    def create_update_dict(self):
        return self.dict(
            exclude_unset=True,
            exclude={
                "id",
                "is_superuser",
                "is_active",
                "is_verified",
                "oauth_accounts",
                "probing_enabled",
                "probing_limit",
                "allow_tag_reserved",
                "allow_tag_public",
            },
        )


class User(models.BaseUser):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 1
    allow_tag_reserved: bool = False
    allow_tag_public: bool = True
    creation_time: datetime = Field(default_factory=datetime.utcnow)


class UserCreate(CustomCreateUpdateDictModel, models.BaseUserCreate):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 1
    allow_tag_reserved: bool = False
    allow_tag_public: bool = True


class UserUpdate(CustomCreateUpdateDictModel, models.BaseUserUpdate):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 1
    allow_tag_reserved: bool = False
    allow_tag_public: bool = True


class UserDB(User, models.BaseUserDB):
    pass


class AWSCredentials(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str
    endpoint_url: str


class ClickHouseCredentials(BaseModel):
    base_url: str
    database: str
    username: str
    password: str


class ExternalServices(BaseModel):
    clickhouse: ClickHouseCredentials
    clickhouse_expiration_time: datetime
    s3: AWSCredentials
    s3_expiration_time: datetime
