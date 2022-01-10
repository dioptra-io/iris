"""initial admin user

Revision ID: 2ea24c3da9ba
Revises: 16e55815e691
Create Date: 2022-01-10 17:00:33.649009

"""
import asyncio
import logging
from uuid import UUID

# revision identifiers, used by Alembic.
from fastapi_users.password import get_password_hash
from sqlalchemy.orm import Session

from alembic import op
from iris.commons.models import UserTable
from iris.commons.settings import CommonSettings
from iris.commons.storage import Storage

revision = "2ea24c3da9ba"
down_revision = "16e55815e691"
branch_labels = None
depends_on = None

# TODO?
# https://fastapi-users.github.io/fastapi-users/cookbook/create-user-programmatically/

settings = CommonSettings()
storage = Storage(settings, logging.getLogger(__name__))


def create_buckets(user_id):
    async def create_buckets_():
        await storage.create_bucket(storage.archive_bucket(str(user_id)))
        await storage.create_bucket(storage.targets_bucket(str(user_id)))

    asyncio.run(create_buckets_())


def delete_buckets(user_id):
    async def delete_buckets_():
        await storage.delete_bucket(storage.archive_bucket(str(user_id)))
        await storage.delete_bucket(storage.targets_bucket(str(user_id)))

    asyncio.run(delete_buckets_())


def upgrade():
    bind = op.get_bind()
    session = Session(bind=bind)
    user = UserTable(
        id=UUID("fb2ebc52-7685-41cc-926a-880e6a939ee2"),
        firstname="admin",
        lastname="admin",
        email="admin@example.org",
        hashed_password=get_password_hash("admin"),
        is_active=True,
        is_verified=True,
        is_superuser=True,
        probing_enabled=1,
        probing_limit=1_000_000,
    )
    session.add(user)
    session.commit()
    create_buckets(user.id)


def downgrade():
    bind = op.get_bind()
    session = Session(bind=bind)
    user = session.get(UserTable, UUID("fb2ebc52-7685-41cc-926a-880e6a939ee2"))
    session.delete(user)
    session.commit()
    delete_buckets(user.id)
