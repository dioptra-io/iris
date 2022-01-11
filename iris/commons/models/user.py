from datetime import datetime
from typing import Optional

from fastapi_users import models
from fastapi_users_db_sqlalchemy import (
    SQLAlchemyBaseOAuthAccountTable,
    SQLAlchemyBaseUserTable,
)
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import relationship

from iris.commons.models.base import Base, BaseModel


class UserTable(Base, SQLAlchemyBaseUserTable):
    oauth_accounts = relationship("OAuthAccountTable")
    firstname: str = Column(String, nullable=False)
    lastname: str = Column(String, nullable=False)
    probing_enabled = Column(Boolean, nullable=False, default=False)
    probing_limit = Column(Integer, nullable=True, default=0)
    allow_tag_reserved = Column(Boolean, nullable=False, default=False)
    allow_tag_public = Column(Boolean, nullable=False, default=False)


class OAuthAccountTable(SQLAlchemyBaseOAuthAccountTable, Base):
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


class User(models.BaseUser, models.BaseOAuthAccountMixin):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 0
    allow_tag_reserved: bool = False
    allow_tag_public: bool = False


class UserCreate(CustomCreateUpdateDictModel, models.BaseUserCreate):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 0
    allow_tag_reserved: bool = False
    allow_tag_public: bool = False


class UserUpdate(CustomCreateUpdateDictModel, models.BaseUserUpdate):
    firstname: str = "string"
    lastname: str = "string"
    probing_enabled: bool = False
    probing_limit: Optional[int] = 0
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
