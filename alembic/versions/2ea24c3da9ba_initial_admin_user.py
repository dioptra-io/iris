"""initial admin user

Revision ID: 2ea24c3da9ba
Revises: 16e55815e691
Create Date: 2022-01-10 17:00:33.649009

"""
import asyncio
import logging
from uuid import UUID

# revision identifiers, used by Alembic.
from fastapi_users.password import PasswordHelper
from sqlalchemy import text
from sqlalchemy.orm import Session

from alembic import op
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


user_id = UUID("fb2ebc52-7685-41cc-926a-880e6a939ee2")

def upgrade():
    connection = op.get_bind()

    with Session(bind=connection) as session:
        hashed_password = PasswordHelper().hash("admin")
        session.execute(
            text(
                """
                INSERT INTO public."user"
                    (id, firstname, lastname, email, hashed_password,
                     is_active, is_verified, is_superuser, probing_enabled, probing_limit)
                VALUES
                    (:id, 'admin', 'admin', 'admin@example.org', :hashed_password,
                     true, true, true, true, 1000000)
                """
            ),
            {"id": str(user_id), "hashed_password": hashed_password},
        )
        session.commit()

    create_buckets(user_id)


def downgrade():
    connection = op.get_bind()

    with Session(bind=connection) as session:
        session.execute(
            text("DELETE FROM public.\"user\" WHERE id = :id"),
            {"id": str(user_id)},
        )
        session.commit()

    delete_buckets(user_id)
