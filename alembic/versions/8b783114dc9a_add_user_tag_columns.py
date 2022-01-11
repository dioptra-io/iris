"""add user tag columns

Revision ID: 8b783114dc9a
Revises: 2ea24c3da9ba
Create Date: 2022-01-11 00:14:35.845955

"""
import sqlalchemy as sa
from sqlalchemy.orm import Session

from alembic import op

# revision identifiers, used by Alembic.
revision = "8b783114dc9a"
down_revision = "2ea24c3da9ba"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "user",
        sa.Column(
            "tag_reserved_allowed",
            sa.Boolean,
            nullable=False,
            server_default=sa.sql.expression.false(),
        ),
    )
    op.alter_column("user", "tag_reserved_allowed", server_default=None)
    op.add_column(
        "user",
        sa.Column(
            "tag_public_allowed",
            sa.Boolean,
            nullable=False,
            server_default=sa.sql.expression.false(),
        ),
    )
    op.alter_column("user", "tag_public_allowed", server_default=None)

    bind = op.get_bind()
    session = Session(bind=bind)
    session.execute(
        "UPDATE public.user SET tag_reserved_allowed = true WHERE is_superuser = true"
    )
    session.execute(
        "UPDATE public.user SET tag_public_allowed = true WHERE is_superuser = true"
    )
    session.commit()


def downgrade():
    op.drop_column("user", "tag_reserved_allowed")
    op.drop_column("user", "tag_public_allowed")
