"""use JSONB instead of JSON

Revision ID: 3bfd4e486e3f
Revises: 9dc1b96c99eb
Create Date: 2022-01-18 11:15:05.138792

"""
# revision identifiers, used by Alembic.
from sqlalchemy.dialects.postgresql import JSON, JSONB

from alembic import op

revision = "3bfd4e486e3f"
down_revision = "9dc1b96c99eb"
branch_labels = None
depends_on = None


def upgrade():
    for column in ["agent_parameters", "tool_parameters", "probing_statistics"]:
        op.alter_column(
            "measurement_agent", column, existing_type=JSON(), type_=JSONB()
        )


def downgrade():
    for column in ["agent_parameters", "tool_parameters", "probing_statistics"]:
        op.alter_column(
            "measurement_agent",
            column,
            existing_type=JSONB(),
            type_=JSON(),
        )
