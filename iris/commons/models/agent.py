from enum import Enum
from ipaddress import IPv4Address, IPv6Address

from pydantic import Field, NonNegativeInt

from iris.commons.models.base import BaseModel


class AgentState(Enum):
    Idle = "idle"
    Unknown = "unknown"
    Working = "working"


class AgentParameters(BaseModel):
    version: str
    hostname: str
    internal_ipv4_address: IPv4Address | None = Field(None, title="Internal IPv4 address")
    internal_ipv6_address: IPv6Address | None = Field(None, title="Internal IPv6 address")
    external_ipv4_address: IPv4Address | None = Field(None, title="External IPv4 address")
    external_ipv6_address: IPv6Address | None = Field(None, title="External IPv6 address")
    cpus: int = Field(title="Number of logical CPUs")
    disk: float = Field(title="Total disk size in GB")
    memory: float = Field(title="Total memory in GB")
    min_ttl: NonNegativeInt = Field(
        title="Minimum TTL",
        description="Minimum TTL allowed by the agent",
    )
    max_probing_rate: NonNegativeInt = Field(
        title="Maximum probing rate",
        description="Maximum Probing Rate allowed by the agent",
    )
    tags: list[str] = Field(default_factory=list)


class Agent(BaseModel):
    uuid: str
    state: AgentState
    parameters: AgentParameters
