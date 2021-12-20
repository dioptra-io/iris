from enum import Enum
from ipaddress import IPv4Address, IPv6Address
from typing import List, Optional
from uuid import UUID

from pydantic import Field, NonNegativeInt

from iris.commons.schemas.base import BaseModel


class AgentParameters(BaseModel):
    """Parameters of an agent (Response)."""

    version: str
    hostname: str
    ipv4_address: Optional[IPv4Address] = Field(..., title="IPv4 address")
    ipv6_address: Optional[IPv6Address] = Field(..., title="IPv4 address")
    min_ttl: NonNegativeInt = Field(
        ...,
        title="Minimum TTL",
        description="Minimum TTL allowed by the agent",
    )
    max_probing_rate: NonNegativeInt = Field(
        ...,
        title="Maximum probing rate",
        description="Maximum Probing Rate allowed by the agent",
    )
    agent_tags: List[str]


class AgentState(str, Enum):
    Idle = "idle"
    Unknown = "unknown"
    Working = "working"


class Agent(BaseModel):
    """Summary information about an agent (Response)."""

    uuid: UUID
    parameters: Optional[AgentParameters]
    state: Optional[AgentState]
