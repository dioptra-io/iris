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
    op.alter_column(
        "measurement_agent",
        "agent_parameters",
        existing_type=sqlmodel.sql.sqltypes.AutoString(),
        type_=JSON(),
        postgresql_using="agent_parameters::json",
    )
    op.alter_column(
        "measurement_agent",
        "tool_parameters",
        existing_type=sqlmodel.sql.sqltypes.AutoString(),
        type_=JSON(),
        postgresql_using="tool_parameters::json",
    )


def downgrade():
    op.alter_column(
        "measurement_agent",
        "agent_parameters",
        existing_type=JSON(),
        type_=sqlmodel.sql.sqltypes.AutoString(),
    )
    op.alter_column(
        "measurement_agent",
        "tool_parameters",
        existing_type=JSON(),
        type_=sqlmodel.sql.sqltypes.AutoString(),
    )
