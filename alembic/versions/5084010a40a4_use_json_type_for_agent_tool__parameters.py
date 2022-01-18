"""use JSON type for {agent,tool}_parameters

Revision ID: 5084010a40a4
Revises: e887befec67e
Create Date: 2022-01-18 10:31:33.181304

"""
import sqlmodel.sql.sqltypes

# revision identifiers, used by Alembic.
from sqlalchemy.dialects.postgresql import JSON

from alembic import op

revision = "5084010a40a4"
down_revision = "e887befec67e"
branch_labels = None
depends_on = None


def upgrade():
    for column in ["agent_parameters", "tool_parameters"]:
        op.alter_column(
            "measurement_agent",
            column,
            existing_type=sqlmodel.sql.sqltypes.AutoString(),
            type_=JSON(),
            postgresql_using=f"{column}::json",
        )


def downgrade():
    for column in ["agent_parameters", "tool_parameters"]:
        op.alter_column(
            "measurement_agent",
            column,
            existing_type=JSON(),
            type_=sqlmodel.sql.sqltypes.AutoString(),
        )
