from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from iris.commons.round import Round
from iris.commons.schemas import public


class MeasurementRequest(public.MeasurementPostBody):
    start_time: datetime = Field(default_factory=datetime.now)
    uuid: UUID = Field(default_factory=uuid4)
    username: str

    def agent(self, uuid: UUID) -> public.MeasurementAgentPostBody:
        for agent in self.agents:
            if agent.uuid == uuid:
                return agent
        raise ValueError(f"no agent found for UUID {uuid}")


class MeasurementRoundRequest(BaseModel):
    measurement: MeasurementRequest
    probes: Optional[str]
    round: Round
