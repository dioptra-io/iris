"""API Body and Response schemas."""

from pydantic import BaseModel, Field
from typing import Optional, List

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

    uuid: str
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
    results: List[AgentSummaryResponse]


class AgentsGetByUUIDResponse(BaseModel):
    """GET /agents/{uuid} (Response)."""

    uuid: str
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

    uuid: str
    state: str
    targets_file_key: str
    full: bool
    start_time: str
    end_time: Optional[str]


class MeasurementsGetResponse(BaseModel):
    """GET /measurements (Response)."""

    count: int
    results: List[MeasurementSummaryResponse]


class MeasurementsPostBody(BaseModel):
    """POST /measurements (Body)."""

    targets_file_key: Optional[str]
    full: Optional[bool]
    agents: List[str] = Field(
        None,
        title="Optional agent list",
        description="Publish the measurement to all agents if not set.",
    )
    protocol: str = Field(
        ...,
        title="Probing transport protocol",
        description="Must be either tcp or udp.",
        regex="(?i)^udp$|^tcp$",
    )
    destination_port: int = Field(..., title="Destination port", ge=1, le=65_535)
    min_ttl: int = Field(1, title="Minimum TTL", gt=0)
    max_ttl: int = Field(30, title="Maximum TTL", gt=0)


class MeasurementsPostResponse(BaseModel):
    """POST /measurements (Response)."""

    uuid: str


class MeasurementAgentInfoResponse(BaseModel):
    """Information about information of agents specific to a measurement (Response)."""

    uuid: str
    state: str
    min_ttl: int
    max_ttl: int
    parameters: AgentParametersSummaryResponse


class MeasurementInfoResponse(BaseModel):
    """Information about a measurement (Response)."""

    uuid: str
    state: str
    agents: List[MeasurementAgentInfoResponse]
    targets_file_key: str
    full: bool
    protocol: str
    destination_port: int
    min_ttl: int
    max_ttl: int
    start_time: str
    end_time: Optional[str]


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
