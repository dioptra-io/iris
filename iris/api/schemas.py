"""API Body and Response schemas."""

from enum import Enum
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


class AgentParametersResponse(BaseModel):
    """Parameters of an agent (Response)."""

    version: str
    hostname: str
    ip_address: str
    min_ttl: int
    max_probing_rate: int


class AgentSummaryResponse(BaseModel):
    """Summary information about a agent (Response)."""

    uuid: UUID
    state: str
    parameters: AgentParametersResponse


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


class TargetSumaryResponse(BaseModel):
    """Information about a target (Response)."""

    key: str
    last_modified: str


class TargetResponse(BaseModel):
    """Information about a target (Response)."""

    key: str
    size: int
    content: List[str]
    last_modified: str


class TargetsGetResponse(BaseModel):
    """GET /targets (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[TargetSumaryResponse]


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


class Protocol(str, Enum):
    udp = "udp"
    icmp = "icmp"


class FlowMapper(str, Enum):
    SequentialFlowMapper = "SequentialFlowMapper"
    IntervalFlowMapper = "IntervalFlowMapper"
    ReverseByteFlowMapper = "ReverseByteFlowMapper"
    RandomFlowMapper = "RandomFlowMapper"


class ToolParameters(BaseModel):
    initial_source_port: int = Field(
        24000, title="Initial source port", gt=0, lt=65_536
    )
    destination_port: int = Field(33434, title="Destination port", gt=0, lt=65_536)
    max_round: int = Field(10, title="Maximum round", gt=0, lt=256)
    flow_mapper: str = Field(FlowMapper.RandomFlowMapper, title="Flow mapper")
    flow_mapper_kwargs: Optional[Dict[str, Any]] = Field(
        {"seed": 42}, title="Optional keyword arguments for the flow mapper"
    )


class MeasurementsAgentsPostBody(BaseModel):
    """POST /measurements (Body)."""

    uuid: UUID
    target_file: str = Field(..., title="Target file key")
    probing_rate: int = Field(None, title="Probing rate")
    tool_parameters: ToolParameters = Field(ToolParameters(), title="Tool parameters")


class Tool(str, Enum):
    diamond_miner = "diamond-miner"
    yarrp = "yarrp"
    ping = "ping"


class MeasurementsPostBody(BaseModel):
    """POST /measurements (Body)."""

    tool: str = Field(Tool.diamond_miner, title="Probing tool")
    agents: List[MeasurementsAgentsPostBody] = Field(
        ...,
        title="Agents participating to the measurement",
        description="Optional agent parameters can also be set",
    )
    tags: List[str] = Field([], title="Tags")


class MeasurementsPostResponse(BaseModel):
    """POST /measurements (Response)."""

    uuid: UUID


class MeasurementAgentSpecific(BaseModel):
    """Information about agent specific information (Response)."""

    target_file: str
    probing_rate: Optional[int]
    tool_parameters: ToolParameters


class MeasurementAgentInfoResponse(BaseModel):
    """Information about information of agents specific to a measurement (Response)."""

    uuid: UUID
    state: str
    specific: MeasurementAgentSpecific
    parameters: AgentParametersResponse


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
    reply_mpls_labels: List[int]
    reply_size: int
    rtt: float
    round: int


class MeasurementsResultsResponse(BaseModel):
    """GET /measurements/{uuid} (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[PacketResponse]
