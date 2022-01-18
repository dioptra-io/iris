"""probing statistics as dict

Revision ID: 03ddf61b5b3c
Revises: 8b783114dc9a
Create Date: 2022-01-11 19:00:16.273260

"""
import sqlmodel.sql.sqltypes

# revision identifiers, used by Alembic.
from sqlalchemy.dialects.postgresql import JSON

from alembic import op

revision = "03ddf61b5b3c"
down_revision = "8b783114dc9a"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "measurement_agent",
        "probing_statistics",
        existing_type=sqlmodel.sql.sqltypes.AutoString(),
        type_=JSON(),
        postgresql_using="probing_statistics::json",
    )


def downgrade():
    op.alter_column(
        "measurement_agent",
        "probing_statistics",
        existing_type=JSON(),
        type_=sqlmodel.sql.sqltypes.AutoString(),
    )
