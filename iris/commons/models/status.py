from iris.commons.models import AgentState, MeasurementAgentState
from iris.commons.models.base import BaseModel


class Status(BaseModel):
    agents: dict[AgentState, int]
    buckets: int
    measurements: dict[MeasurementAgentState, int]
    version: str
