from datetime import datetime
from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import Column
from sqlalchemy_utils import ScalarListType
from sqlmodel import Field, SQLModel

from iris.commons.schemas.measurements import MeasurementState, Tool


class Measurement2(SQLModel, table=True):
    uuid: UUID = Field(default_factory=uuid4, primary_key=True)
    user_id: UUID
    tool: Tool
    # TODO: Proper tag ang measurement_tag tables?
    tags: List[str] = Field(sa_column=Column(ScalarListType))
    state: MeasurementState
    start_time: datetime
    end_time: Optional[datetime]
