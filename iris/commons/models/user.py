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
    allow_tag_public: bool = False
    creation_time: datetime = Field(default_factory=datetime.utcnow)


class UserCreate(CustomCreateUpdateDictModel, models.BaseUserCreate):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 1
    allow_tag_reserved: bool = False
    allow_tag_public: bool = False


class UserUpdate(CustomCreateUpdateDictModel, models.BaseUserUpdate):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 1
    allow_tag_reserved: bool = False
    allow_tag_public: bool = False


class UserDB(User, models.BaseUserDB):
    pass


class ExternalServices(BaseModel):
    chproxy_url: str
    chproxy_username: str
    chproxy_password: str
    s3_host: str
    s3_access_key_expiration: datetime
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_session_token: str
