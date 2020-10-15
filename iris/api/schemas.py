"""API Body and Response schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List
from uuid import UUID

# --- Commons ----


class ExceptionResponse(BaseModel):
    """Generic exception (Response)."""

    detail: str


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
    pfring: bool


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
    min_ttl: int = Field(1, title="Minimum TTL", gt=0)
    max_ttl: int = Field(30, title="Maximum TTL", gt=0)
    probing_rate: int = Field(None, title="Probing Rate", gt=0)
    max_round: int = Field(10, title="Maximum round", gt=0, lt=256)


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
        description="Must be either tcp or udp.",
        regex="(?i)^udp$|^tcp$",
    )
    destination_port: int = Field(..., title="Destination port", gt=0, lt=65_536)
    min_ttl: int = Field(1, title="Minimum TTL", gt=0, lt=256)
    max_ttl: int = Field(30, title="Maximum TTL", gt=0, lt=256)
    max_round: int = Field(10, title="Maximum round", gt=0, lt=256)


class MeasurementsPostResponse(BaseModel):
    """POST /measurements (Response)."""

    uuid: UUID


class MeasurementAgentSpecific(BaseModel):
    """Information about agent specific information (Response)."""

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
    targets_file_key: Optional[str]
    full: bool
    protocol: str
    destination_port: int
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
    # snapshot: int # Not curently used


class MeasurementsResultsResponse(BaseModel):
    """GET /measurements/{uuid} (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[PacketResponse]
