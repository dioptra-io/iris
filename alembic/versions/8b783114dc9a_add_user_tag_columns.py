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
            "allow_tag_reserved",
            sa.Boolean,
            nullable=False,
            server_default=sa.sql.expression.false(),
        ),
    )
    op.alter_column("user", "allow_tag_reserved", server_default=None)
    op.add_column(
        "user",
        sa.Column(
            "allow_tag_public",
            sa.Boolean,
            nullable=False,
            server_default=sa.sql.expression.false(),
        ),
    )
    op.alter_column("user", "allow_tag_public", server_default=None)

    connection = op.get_bind()
    with Session(bind=connection) as session:
        session.execute(
            text('UPDATE public."user" SET allow_tag_reserved = true WHERE is_superuser = true')
        )
        session.execute(
            text('UPDATE public."user" SET allow_tag_public = true WHERE is_superuser = true')
        )
        session.commit()

def downgrade():
    op.drop_column("user", "allow_tag_reserved")
    op.drop_column("user", "allow_tag_public")
