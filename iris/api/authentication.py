"""Security management."""
import uuid

from fastapi import Depends, Request
from fastapi_users import BaseUserManager, FastAPIUsers, UUIDIDMixin
from fastapi_users.authentication import (
    AuthenticationBackend,
    BearerTransport,
    CookieTransport,
    JWTStrategy,
)
from fastapi_users.authentication.strategy import DatabaseStrategy
from fastapi_users.db import SQLAlchemyUserDatabase

from iris.api.settings import APISettings
from iris.commons.dependencies import (
    get_access_token_db,
    get_settings,
    get_storage,
    get_user_db,
)
from iris.commons.models import User
from iris.commons.storage import Storage


class UserManager(UUIDIDMixin, BaseUserManager[User, uuid.UUID]):
    def __init__(
        self, user_db, storage, reset_password_token_secret, verification_token_secret
    ):
        super().__init__(user_db)
        self.storage = storage
        self.reset_password_token_secret = reset_password_token_secret
        self.verification_token_secret = verification_token_secret

    async def on_after_register(self, user: User, request: Request | None = None):
        """
        After user registration hook.
        :param user: The newly registered user.
        :param request: The request that triggered the registration.
        """
        # Create the buckets for the user
        await self.storage.create_bucket(self.storage.targets_bucket(str(user.id)))
        await self.storage.create_bucket(self.storage.archive_bucket(str(user.id)))

    async def delete(self, user: User) -> None:
        """
        Delete a user.
        :param user: The user to delete.
        """
        await self.user_db.delete(user)
        await self.on_after_delete(user)

    async def on_after_delete(self, user: User) -> None:
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
    settings: APISettings = Depends(get_settings),
    storage: Storage = Depends(get_storage),
):
    yield UserManager(
        user_db,
        storage=storage,
        reset_password_token_secret=settings.API_JWT_SECRET_KEY,
        verification_token_secret=settings.API_JWT_SECRET_KEY,
    )


def get_database_strategy(
    access_token_db=Depends(get_access_token_db),
):
    return DatabaseStrategy(
        access_token_db, lifetime_seconds=settings.API_COOKIE_LIFETIME
    )


def get_jwt_strategy(settings: APISettings = Depends(get_settings)) -> JWTStrategy:
    return JWTStrategy(
        secret=settings.API_JWT_SECRET_KEY,
        lifetime_seconds=settings.API_JWT_LIFETIME,
    )


# TODO: DI for cookie settings?
settings = APISettings()

bearer_transport = BearerTransport(tokenUrl="auth/jwt/login")
cookie_transport = CookieTransport(
    cookie_domain=settings.API_COOKIE_DOMAIN,
    cookie_max_age=settings.API_COOKIE_LIFETIME,
    cookie_samesite=settings.API_COOKIE_SAMESITE,
)

cookie_auth_backend = AuthenticationBackend(
    name="cookie",
    transport=cookie_transport,
    get_strategy=get_database_strategy,
)

jwt_auth_backend = AuthenticationBackend(
    name="jwt",
    transport=bearer_transport,
    get_strategy=get_jwt_strategy,
)

fastapi_users = FastAPIUsers(get_user_manager, [cookie_auth_backend, jwt_auth_backend])

current_active_user = fastapi_users.current_user(active=True)
current_verified_user = fastapi_users.current_user(active=True, verified=True)
current_superuser = fastapi_users.current_user(
    active=True, verified=True, superuser=True
)
