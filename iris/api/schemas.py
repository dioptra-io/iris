"""API Body and Response schemas."""

from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

# --- Commons ----


class ExceptionResponse(BaseModel):
    """Generic exception (Response)."""

    detail: str


# --- Profile ---


class ProfileRIPEPutResponse(BaseModel):
    """Profile RIPE information (Response)."""

    account: Optional[str]
    key: Optional[str]


class ProfileRIPEPutBody(BaseModel):
    """Profile RIPE information (Body)."""

    account: Optional[str]
    key: Optional[str]


class ProfileGetResponse(BaseModel):
    """Profile nformation (Response)."""

    uuid: UUID
    username: str
    email: str
    is_active: bool
    is_admin: bool
    is_full_capable: bool
    register_date: str
    ripe: ProfileRIPEPutResponse


# --- Agents ---


class AgentParametersSummaryResponse(BaseModel):
    """Summary parameters information about a agent (Response)."""

    version: str
    hostname: str
    ip_address: str
    probing_rate: int


class AgentSummaryResponse(BaseModel):
    """Summary information about a agent (Response)."""

    uuid: UUID
    state: str
    parameters: AgentParametersSummaryResponse


class AgentParametersResponse(BaseModel):
    """Parameters of an agent (Response)."""

    version: str
    hostname: str
    ip_address: str
    probing_rate: int
    buffer_sniffer_size: int
    inf_born: int
    sup_born: int
    ips_per_subnet: int


class AgentsGetResponse(BaseModel):
    """GET /agents (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[AgentSummaryResponse]


class AgentsGetByUUIDResponse(BaseModel):
    """GET /agents/{uuid} (Response)."""

    uuid: UUID
    state: str
    parameters: AgentParametersResponse


# --- Targets ---


class TargetResponse(BaseModel):
    """Information about a target (Response)."""

    key: str
    type: str
    size: int
    last_modified: str


class TargetsGetResponse(BaseModel):
    """GET /targets (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[TargetResponse]


class TargetsPostResponse(BaseModel):
    """POST /targets (Response)."""

    key: str
    type: str
    action: str


class TargetsDeleteResponse(BaseModel):
    """DELETE /targets (Response)."""

    key: str
    action: str


# --- Measurements ----


class MeasurementSummaryResponse(BaseModel):
    """Summary information about a measurement (Response)."""

    uuid: UUID
    state: str
    targets_file_key: Optional[str]
    full: bool
    tags: List[str]
    start_time: str
    end_time: Optional[str]


class MeasurementsGetResponse(BaseModel):
    """GET /measurements (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[MeasurementSummaryResponse]


class MeasurementsAgentsPostBody(BaseModel):
    """POST /measurements (Body)."""

    uuid: UUID
    targets_file_key: str = Field(None, title="Target file key")
    min_ttl: int = Field(None, title="Minimum TTL", gt=0)
    max_ttl: int = Field(None, title="Maximum TTL", gt=0)
    probing_rate: int = Field(None, title="Probing Rate", gt=0)
    max_round: int = Field(None, title="Maximum round", gt=0, lt=256)
    flow_mapper: str = Field(None, title="Flow mapper")
    flow_mapper_kwargs: Dict[str, Any] = Field(
        None, title="Optional keyword arguments for the flow mapper"
    )


class MeasurementsPostBody(BaseModel):
    """POST /measurements (Body)."""

    targets_file_key: Optional[str]
    full: Optional[bool]
    agents: List[MeasurementsAgentsPostBody] = Field(
        None,
        title="Optional agent specific parameters",
        description="If not set, publish the measurement to all agents.",
    )
    protocol: str = Field(
        ...,
        title="Probing transport protocol",
        description="Must be either udp or icmp.",
        regex="(?i)^udp$|^icmp$",
    )
    destination_port: int = Field(..., title="Destination port", gt=0, lt=65_536)
    min_ttl: int = Field(1, title="Minimum TTL", gt=0, lt=256)
    max_ttl: int = Field(30, title="Maximum TTL", gt=0, lt=256)
    max_round: int = Field(10, title="Maximum round", gt=0, lt=256)
    flow_mapper: str = Field("IntervalFlowMapper", title="Flow mapper")
    flow_mapper_kwargs: Dict[str, Any] = Field(
        None, title="Optional keyword arguments for the flow mapper"
    )
    tags: List[str] = Field([], title="Tags")


class MeasurementsPostResponse(BaseModel):
    """POST /measurements (Response)."""

    uuid: UUID


class MeasurementAgentSpecific(BaseModel):
    """Information about agent specific information (Response)."""

    targets_file_key: Optional[str]
    min_ttl: int
    max_ttl: int
    probing_rate: int
    max_round: int


class MeasurementAgentParameters(BaseModel):
    """Summary parameters information about a agent (Response)."""

    version: str
    hostname: str
    ip_address: str


class MeasurementAgentInfoResponse(BaseModel):
    """Information about information of agents specific to a measurement (Response)."""

    uuid: UUID
    state: str
    specific: MeasurementAgentSpecific
    parameters: MeasurementAgentParameters


class MeasurementInfoResponse(BaseModel):
    """Information about a measurement (Response)."""

    uuid: UUID
    state: str
    agents: List[MeasurementAgentInfoResponse]
    full: bool
    protocol: str
    destination_port: int
    tags: List[str]
    start_time: str
    end_time: Optional[str]


class MeasurementsDeleteResponse(BaseModel):

    uuid: UUID
    action: str


class PacketResponse(BaseModel):
    """Full information about a measurement (Response)."""

    source_ip: str
    destination_prefix: str
    destination_ip: str
    reply_ip: str
    protocol: str
    source_port: int
    destination_port: int
    ttl: int
    ttl_check: int  # implemented only in UDP
    type: int
    code: int
    rtt: float
    reply_ttl: int
    reply_size: int
    round: int
    # snapshot: int # NOTE Not curently used


class MeasurementsResultsResponse(BaseModel):
    """GET /measurements/{uuid} (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[PacketResponse]
