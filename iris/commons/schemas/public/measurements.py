from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, root_validator

from iris.commons.schemas.base import BaseModel
from iris.commons.schemas.public.agents import AgentParameters


class FlowMapper(str, Enum):
    SequentialFlowMapper = "SequentialFlowMapper"
    IntervalFlowMapper = "IntervalFlowMapper"
    ReverseByteFlowMapper = "ReverseByteFlowMapper"
    RandomFlowMapper = "RandomFlowMapper"


class ProbingStatistics(BaseModel):
    round: str
    statistics: Dict[str, Any]


class Tool(str, Enum):
    DiamondMiner = "diamond-miner"
    Yarrp = "yarrp"
    Ping = "ping"


class ToolParameters(BaseModel):
    initial_source_port: int = Field(
        24000, title="Initial source port", gt=0, lt=65_536
    )
    destination_port: int = Field(33434, title="Destination port", gt=0, lt=65_536)
    max_round: int = Field(10, title="Maximum round", gt=0, lt=256)
    n_flow_ids: int = Field(6, title="Number of flow IDs to probe at round 1")
    flow_mapper: str = Field(FlowMapper.RandomFlowMapper, title="Flow mapper")
    flow_mapper_kwargs: Optional[Dict[str, Any]] = Field(
        {"seed": 42}, title="Optional keyword arguments for the flow mapper"
    )
    global_min_ttl: int = Field(
        0, ge=0, le=255, title="Do not set. Overridden by the API."
    )
    global_max_ttl: int = Field(
        255, ge=0, le=255, title="Do not set. Overridden by the API."
    )


class MeasurementState(str, Enum):
    Canceled = "canceled"
    Finished = "finished"
    Ongoing = "ongoing"
    Unknown = "unknown"
    Waiting = "waiting"


class MeasurementSummary(BaseModel):
    """Summary information about a measurement (Response)."""

    uuid: UUID
    state: MeasurementState
    tool: Tool
    tags: List[str]
    start_time: str
    end_time: Optional[str]


class MeasurementAgentSpecific(BaseModel):
    """Information about agent specific information (Response)."""

    target_file: str
    target_file_content: List[str]
    probing_rate: Optional[int]
    tool_parameters: ToolParameters


class MeasurementAgent(BaseModel):
    """Information about information of agents specific to a measurement (Response)."""

    uuid: UUID
    state: MeasurementState
    specific: MeasurementAgentSpecific
    parameters: AgentParameters
    probing_statistics: List[ProbingStatistics]


class Measurement(BaseModel):
    """Information about a measurement (Response)."""

    uuid: UUID
    state: MeasurementState
    tool: Tool
    agents: List[MeasurementAgent]
    tags: List[str]
    start_time: str
    end_time: Optional[str]


class MeasurementAgentPostBody(BaseModel):
    """POST /measurements (Body)."""

    uuid: Optional[UUID]
    tag: Optional[str]
    target_file: str = Field(..., title="Target file key")
    probing_rate: int = Field(None, title="Probing rate")
    tool_parameters: ToolParameters = Field(ToolParameters(), title="Tool parameters")

    @root_validator
    def check_uuid_or_tag(cls, values):
        uuid, tag = values.get("uuid"), values.get("tag")
        if bool(uuid) != bool(tag):
            return values
        raise ValueError("one of `uuid` or `tag` must be specified")


class MeasurementPostBody(BaseModel):
    """POST /measurements (Body)."""

    tool: Tool = Field(Tool.DiamondMiner, title="Probing tool")
    agents: List[MeasurementAgentPostBody] = Field(
        ...,
        title="Agents participating to the measurement",
        description="Optional agent parameters can also be set",
    )
    tags: List[str] = Field([], title="Tags")


class MeasurementPostResponse(BaseModel):
    """POST /measurements (Response)."""

    uuid: UUID


class MeasurementDeleteResponse(BaseModel):
    """DELETE /measurements/{uuid} (Response)."""

    uuid: UUID
    action: str
