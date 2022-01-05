"""Security management."""

from typing import Optional

from fastapi import Depends, HTTPException, Request
from fastapi_users import BaseUserManager, FastAPIUsers, models
from fastapi_users.authentication import JWTAuthentication
from fastapi_users.db import SQLAlchemyUserDatabase
from fastapi_users.jwt import generate_jwt
from starlette import status

from iris.api.dependencies import get_settings, get_storage, get_user_db
from iris.commons.models.user import User, UserCreate, UserDB, UserUpdate
from iris.commons.storage import Storage

# TODO: DI for settings in this module?
settings = get_settings()


class UserManager(BaseUserManager[UserCreate, UserDB]):
    user_db_model = UserDB
    reset_password_token_secret = settings.API_TOKEN_SECRET_KEY
    verification_token_secret = settings.API_TOKEN_SECRET_KEY

    def __init__(self, user_db, storage):
        super().__init__(user_db)
        self.storage = storage

    async def on_after_register(self, user: UserDB, request: Optional[Request] = None):
        """
        After user registration hook.
        :param user: The newly registered user.
        :param request: The request that triggered the registration.
        """
        # Create the buckets for the user
        await self.storage.create_bucket(self.storage.targets_bucket(str(user.id)))
        await self.storage.create_bucket(self.storage.archive_bucket(str(user.id)))

    async def delete(self, user: UserDB) -> None:
        """
        Delete a user.
        :param user: The user to delete.
        """
        await self.user_db.delete(user)
        await self.on_after_delete(user)

    async def on_after_delete(self, user: UserDB) -> None:
        """
        Perform cleanup after a user is deleted.
        :param user: The user that has been deleted.
        """
        # Remove all files from the storage
        await self.storage.delete_all_files_from_bucket(
            self.storage.archive_bucket(str(user.id))
        )
        await self.storage.delete_all_files_from_bucket(
            self.storage.targets_bucket(str(user.id))
        )

        # Remove user's buckets
        await self.storage.delete_bucket(self.storage.archive_bucket(str(user.id)))
        await self.storage.delete_bucket(self.storage.targets_bucket(str(user.id)))


async def get_user_manager(
    user_db: SQLAlchemyUserDatabase = Depends(get_user_db),
    storage: Storage = Depends(get_storage),
):
    yield UserManager(user_db, storage=storage)


class CustomJWTAuthentication(JWTAuthentication):
    async def _generate_token(self, user: UserDB) -> str:
        data = {
            "user_id": str(user.id),
            "is_active": user.is_active,
            "is_verified": user.is_verified,
            "is_superuser": user.is_superuser,
            "probing_enabled": user.probing_enabled,
            "aud": self.token_audience,
        }
        return generate_jwt(data, self.secret, self.lifetime_seconds)


jwt_authentication = CustomJWTAuthentication(
    secret=settings.API_TOKEN_SECRET_KEY,
    lifetime_seconds=settings.API_TOKEN_LIFETIME,
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
current_superuser = fastapi_users.current_user(
    active=True, verified=True, superuser=True
)


def assert_probing_enabled(user: UserDB):
    if not user.probing_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You must have probing enabled to access this resource",
        )
