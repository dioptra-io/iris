from enum import Enum
from ipaddress import IPv4Address, IPv6Address
from typing import List, Optional

from pydantic import Field, NonNegativeInt

from iris.commons.models.base import BaseModel


class AgentState(Enum):
    Idle = "idle"
    Unknown = "unknown"
    Working = "working"


class AgentParameters(BaseModel):
    version: str
    hostname: str
    ipv4_address: Optional[IPv4Address] = Field(title="IPv4 address")
    ipv6_address: Optional[IPv6Address] = Field(title="IPv6 address")
    min_ttl: NonNegativeInt = Field(
        title="Minimum TTL",
        description="Minimum TTL allowed by the agent",
    )
    max_probing_rate: NonNegativeInt = Field(
        title="Maximum probing rate",
        description="Maximum Probing Rate allowed by the agent",
    )
    tags: List[str] = Field(default_factory=list)


class Agent(BaseModel):
    uuid: str
    state: AgentState
    parameters: AgentParameters
