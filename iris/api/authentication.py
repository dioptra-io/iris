"""Security management."""

from typing import Optional

from fastapi import Depends, Request
from fastapi_users import BaseUserManager, FastAPIUsers, models
from fastapi_users.authentication import JWTAuthentication
from fastapi_users.db import SQLAlchemyUserDatabase
from fastapi_users.jwt import generate_jwt

from iris.api.dependencies import get_session, get_storage, settings
from iris.commons.mail import Mail
from iris.commons.schemas.public import User, UserCreate, UserDB, UserUpdate


class UserManager(BaseUserManager[UserCreate, UserDB]):
    user_db_model = UserDB
    reset_password_token_secret = settings.API_TOKEN_SECRET_KEY
    verification_token_secret = settings.API_TOKEN_SECRET_KEY

    async def on_after_register(self, user: UserDB, request: Optional[Request] = None):
        """
        After user registration hook.
        :param user: The newly registered user.
        :param request: The request that triggered the registration.
        """
        storage = get_storage()

        # Create the buckets for the user
        await storage.create_bucket(storage.targets_bucket(user.id))
        await storage.create_bucket(storage.archive_bucket(user.id))

        # Send the verification email
        if settings.MAIL_ENABLE:
            mail = Mail(settings)
            await mail.send(user.email)

    async def delete(self, user: models.UD) -> None:
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
        storage = get_storage()

        # Remove all files from the storage
        await storage.delete_all_files_from_bucket(storage.archive_bucket(user.id))
        await storage.delete_all_files_from_bucket(storage.targets_bucket(user.id))

        # Remove user's buckets
        await storage.delete_bucket(storage.archive_bucket(user.id))
        await storage.delete_bucket(storage.targets_bucket(user.id))


async def get_user_manager(user_db: SQLAlchemyUserDatabase = Depends(get_session)):
    yield UserManager(user_db)


class CustomJWTAuthentication(JWTAuthentication):
    async def _generate_token(self, user: models.UD) -> str:
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
