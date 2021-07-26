from typing import List
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


class Agent(BaseModel):
    """Summary information about a agent (Response)."""

    uuid: UUID
    state: str
    parameters: AgentParameters
