from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from pydantic import Field

from iris.commons.schemas import public
from iris.commons.schemas.base import BaseModel


class MeasurementRequest(public.MeasurementPostBody):
    start_time: datetime = Field(
        default_factory=lambda: datetime.now().replace(microsecond=0)
    )
    uuid: UUID = Field(default_factory=uuid4)
    username: str

    def agent(self, uuid: UUID) -> public.MeasurementAgentPostBody:
        for agent in self.agents:
            if agent.uuid == uuid:
                return agent
        raise ValueError(f"no agent found for UUID {uuid}")


class MeasurementRoundRequest(BaseModel):
    measurement: MeasurementRequest
    prefix_filename: Optional[str]
    probe_filename: Optional[str]
    round: public.Round
