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
    quota: int
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
    tool: str
    tags: List[str]
    start_time: str
    end_time: Optional[str]


class MeasurementsGetResponse(BaseModel):
    """GET /measurements (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[MeasurementSummaryResponse]


class ToolParameters(BaseModel):
    protocol: str = Field(
        "udp",
        title="Probing transport protocol",
        description="Must be either udp or icmp.",
        regex="(?i)^udp$|^icmp$",
    )
    initial_source_port: int = Field(
        24000, title="Initial source port", gt=0, lt=65_536
    )
    destination_port: int = Field(34334, title="Destination port", gt=0, lt=65_536)
    min_ttl: int = Field(1, title="Minimum TTL", gt=0, lt=256)
    max_ttl: int = Field(30, title="Maximum TTL", gt=0, lt=256)
    max_round: int = Field(10, title="Maximum round", gt=0, lt=256)
    flow_mapper: str = Field(
        "IntervalFlowMapper",
        title="Flow mapper",
        regex=(
            r"(?i)^SequentialFlowMapper$"
            r"|^IntervalFlowMapper$"
            r"|^ReverseByteFlowMapper$"
            r"|^RandomFlowMapper$"
        ),
    )
    flow_mapper_kwargs: Dict[str, Any] = Field(
        None,
        title="Optional keyword arguments for the flow mapper",
    )


class MeasurementsAgentsPostBody(BaseModel):
    """POST /measurements (Body)."""

    uuid: UUID
    targets_file: str = Field(None, title="Target file key")
    probing_rate: int = Field(None, title="Probing rate")
    tool_parameters: ToolParameters = Field(ToolParameters(), title="Tool parameters")


class MeasurementsPostBody(BaseModel):
    """POST /measurements (Body)."""

    targets_file: str = Field(..., title="Target file key")
    tool: str = Field(
        "diamond-miner",
        title="Probing tool",
        regex=r"(?i)^diamond-miner$|^diamond-miner-ping$",
    )
    tool_parameters: ToolParameters = Field(ToolParameters(), title="Tool parameters")
    agents: List[MeasurementsAgentsPostBody] = Field(
        None,
        title="Optional agent specific parameters",
        description="If not set, publish the measurement to all agents.",
    )
    tags: List[str] = Field([], title="Tags")


class MeasurementsPostResponse(BaseModel):
    """POST /measurements (Response)."""

    uuid: UUID


class MeasurementAgentSpecific(BaseModel):
    """Information about agent specific information (Response)."""

    targets_file: str
    probing_rate: int
    tool_parameters: ToolParameters


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
    tool: str
    agents: List[MeasurementAgentInfoResponse]
    tags: List[str]
    start_time: str
    end_time: Optional[str]


class MeasurementsDeleteResponse(BaseModel):

    uuid: UUID
    action: str


class PacketResponse(BaseModel):
    """Probe response information (Response)."""

    probe_src_addr: str
    probe_dst_addr: str
    probe_src_port: int
    probe_dst_port: int
    probe_ttl_l3: int
    probe_ttl_l4: int
    reply_src_addr: str
    reply_protocol: int
    reply_icmp_type: int
    reply_icmp_code: int
    reply_ttl: int
    reply_size: int
    rtt: float
    round: int


class MeasurementsResultsResponse(BaseModel):
    """GET /measurements/{uuid} (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[PacketResponse]
