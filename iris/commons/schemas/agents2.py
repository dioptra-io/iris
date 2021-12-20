from datetime import datetime
from typing import Optional
from uuid import UUID

from sqlmodel import Field, SQLModel

from iris.commons.schemas.measurements import MeasurementState


class AgentDatabase(SQLModel, table=True):
    measurement_uuid: UUID = Field(primary_key=True)
    agent_uuid: UUID = Field(primary_key=True)
    target_file: str
    probing_rate: Optional[int]
    probing_statistics: str
    agent_parameters: str
    tool_parameters: str
    state: MeasurementState
    timestamp: datetime
