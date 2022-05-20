import uuid
from datetime import datetime
from typing import Optional

from fastapi_users import schemas
from fastapi_users_db_sqlalchemy import SQLAlchemyBaseUserTableUUID
from fastapi_users_db_sqlalchemy.access_token import SQLAlchemyBaseAccessTokenTableUUID
from pydantic import Field
from sqlalchemy import Boolean, Column, DateTime, Integer, String

from iris.commons.models.base import Base, BaseModel


class AccessToken(SQLAlchemyBaseAccessTokenTableUUID, Base):
    pass


class User(SQLAlchemyBaseUserTableUUID, Base):
    firstname = Column(String, nullable=False)
    lastname = Column(String, nullable=False)
    probing_enabled = Column(Boolean, nullable=False, default=False)
    probing_limit = Column(Integer, nullable=True, default=1)
    allow_tag_reserved = Column(Boolean, nullable=False, default=False)
    allow_tag_public = Column(Boolean, nullable=False, default=False)
    creation_time = Column(DateTime, nullable=False, default=datetime.utcnow)


class CustomCreateUpdateDictModel(schemas.BaseModel):
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


class UserRead(schemas.BaseUser[uuid.UUID]):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 1
    allow_tag_reserved: bool = False
    allow_tag_public: bool = True
    creation_time: datetime = Field(default_factory=datetime.utcnow)


class UserCreate(CustomCreateUpdateDictModel, schemas.BaseUserCreate):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 1
    allow_tag_reserved: bool = False
    allow_tag_public: bool = True


class UserUpdate(CustomCreateUpdateDictModel, schemas.BaseUserUpdate):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 1
    allow_tag_reserved: bool = False
    allow_tag_public: bool = True


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
