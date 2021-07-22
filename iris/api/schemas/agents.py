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


class Agent(BaseModel):
    """Summary information about a agent (Response)."""

    uuid: UUID
    state: str
    parameters: AgentParameters


class Agents(BaseModel):
    """GET /agents (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[Agent]
