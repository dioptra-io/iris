"""Users operations"""

from typing import Optional

from fastapi import Depends, Request
from fastapi_users import BaseUserManager, FastAPIUsers
from fastapi_users.authentication import JWTAuthentication
from fastapi_users.db import SQLAlchemyUserDatabase

from iris.api.dependencies import get_session, get_storage, settings
from iris.commons.schemas.public import User, UserCreate, UserDB, UserUpdate


class UserManager(BaseUserManager[UserCreate, UserDB]):
    user_db_model = UserDB
    reset_password_token_secret = settings.API_TOKEN_SECRET_KEY
    verification_token_secret = settings.API_TOKEN_SECRET_KEY

    async def on_after_register(self, user: UserDB, request: Optional[Request] = None):
        """After user registration hook."""
        storage = get_storage()
        await storage.create_bucket(storage.targets_bucket(user.id))
        await storage.create_bucket(storage.archive_bucket(user.id))


async def get_user_manager(user_db: SQLAlchemyUserDatabase = Depends(get_session)):
    yield UserManager(user_db)


jwt_authentication = JWTAuthentication(
    secret=settings.API_TOKEN_SECRET_KEY,
    lifetime_seconds=3600,
    tokenUrl="auth/jwt/login",
)

fastapi_users = FastAPIUsers(
    get_user_manager,
    [jwt_authentication],
    User,
    UserCreate,
    UserUpdate,
    UserDB,
)

current_active_user = fastapi_users.current_user(active=True)
current_verified_user = fastapi_users.current_user(active=True, verified=True)
current_super_user = fastapi_users.current_user(
    active=True, verified=True, superuser=True
)
