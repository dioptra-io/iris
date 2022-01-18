from typing import Dict

from iris.commons.models import AgentState, MeasurementAgentState
from iris.commons.models.base import BaseModel


class Status(BaseModel):
    agents: Dict[AgentState, int]
    buckets: int
    measurements: Dict[MeasurementAgentState, int]
    version: str
