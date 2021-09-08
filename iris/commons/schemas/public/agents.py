from enum import Enum
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel


class AgentParameters(BaseModel):
    """Parameters of an agent (Response)."""

    version: str
    hostname: str
    ipv4_address: str
    ipv6_address: str
    min_ttl: int
    max_probing_rate: int
    agent_tags: List[str]


class AgentState(str, Enum):
    Idle = "idle"
    Unknown = "unknown"
    Working = "working"


class Agent(BaseModel):
    """Summary information about a agent (Response)."""

    uuid: UUID
    parameters: Optional[AgentParameters]
    state: Optional[AgentState]
