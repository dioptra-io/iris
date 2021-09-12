from enum import Enum
from ipaddress import IPv4Address, IPv6Address
from typing import List, Optional
from uuid import UUID

from pydantic import NonNegativeInt

from iris.commons.schemas.base import BaseModel


class AgentParameters(BaseModel):
    """Parameters of an agent (Response)."""

    version: str
    hostname: str
    ipv4_address: Optional[IPv4Address]
    ipv6_address: Optional[IPv6Address]
    min_ttl: NonNegativeInt
    max_probing_rate: NonNegativeInt
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
