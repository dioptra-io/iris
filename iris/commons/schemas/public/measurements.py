import re
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, root_validator

from iris.commons.schemas.base import BaseModel
from iris.commons.schemas.public.agents import AgentParameters


class Round(BaseModel):
    number: int = Field(ge=1)
    limit: int = Field(ge=0)
    offset: int = Field(ge=0)

    def encode(self) -> str:
        return f"{self.number}:{self.limit}:{self.offset}"

    @classmethod
    def decode(cls, encoded: str):
        if m := re.match(r".*?(\d+):(\d+):(\d+).*", encoded):
            number, limit, offset = m.groups()
            return cls(number=int(number), limit=int(limit), offset=int(offset))
        raise ValueError(f"cannot decode {encoded}")

    @property
    def min_ttl(self):
        return (self.limit * self.offset) + 1

    @property
    def max_ttl(self):
        if self.limit == 0:
            return 255
        return self.limit * (self.offset + 1)

    def next_round(self, global_max_ttl=0):
        new_round = Round(number=self.number + 1, limit=0, offset=0)
        if self.number > 1:
            # We are not in round 1
            return new_round
        if self.limit == 0:
            # The round 1 has no limit
            return new_round
        if self.limit * (self.offset + 1) >= global_max_ttl:
            # The round 1 has reached the global max ttl
            return new_round
        return Round(number=self.number, limit=self.limit, offset=self.offset + 1)


class FlowMapper(str, Enum):
    SequentialFlowMapper = "SequentialFlowMapper"
    IntervalFlowMapper = "IntervalFlowMapper"
    ReverseByteFlowMapper = "ReverseByteFlowMapper"
    RandomFlowMapper = "RandomFlowMapper"


class ProbingStatistics(BaseModel):
    round: Round
    filtered_low_ttl: int
    filtered_high_ttl: int
    filtered_prefix_excl: int
    filtered_prefix_not_incl: int
    probes_read: int
    packets_sent: int
    packets_failed: int
    packets_received: int
    packets_received_invalid: int


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
    prefix_len_v4: int = Field(24, ge=0, le=32, title="Target prefix length")
    prefix_len_v6: int = Field(64, ge=0, le=128, title="Target prefix length")
    global_min_ttl: int = Field(
        0, ge=0, le=255, title="Do not set. Overridden by the API."
    )
    global_max_ttl: int = Field(
        255, ge=0, le=255, title="Do not set. Overridden by the API."
    )

    @property
    def prefix_size_v4(self):
        return 2 ** (32 - self.prefix_len_v4)

    @property
    def prefix_size_v6(self):
        return 2 ** (128 - self.prefix_len_v6)


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
    start_time: datetime
    end_time: Optional[datetime]


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
    username: str
    state: MeasurementState
    tool: Tool
    agents: List[MeasurementAgent]
    tags: List[str]
    start_time: datetime
    end_time: Optional[datetime]


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

    @root_validator
    def check_tool_parameters(cls, values):
        agents: List[MeasurementAgentPostBody] = values.get("agents")
        tool: Tool = values.get("tool")
        for agent in agents:
            if tool == Tool.DiamondMiner:
                if agent.tool_parameters.n_flow_ids != 6:
                    raise ValueError("`n_flow_ids` must be 6 for diamond-miner")
                # NOTE: We could use other values, but this would require to change
                # the Diamond-Miner results schema which has a materialized column
                # for the destination prefix which assumes /24 and /64 prefixes.
                if agent.tool_parameters.prefix_len_v4 != 24:
                    raise ValueError("`prefix_len_v4` must be 24 for diamond-miner")
                if agent.tool_parameters.prefix_len_v6 != 64:
                    raise ValueError("`prefix_len_v6` must be 64 for diamond-miner")
            if tool == Tool.Ping:
                # NOTE: Technically we could use a larger prefix length to allow
                # the flow mapper to choose a random IP address inside the prefix,
                # but users probably expect ping to target a specific IP address.
                if agent.tool_parameters.prefix_len_v4 != 32:
                    raise ValueError("`prefix_len_v4` must be 32 for ping")
                if agent.tool_parameters.prefix_len_v6 != 128:
                    raise ValueError("`prefix_len_v6` must be 128 for ping")
            if tool in [tool.Ping, tool.Yarrp]:
                # NOTE: We could allow Yarrp to perform multiple flows.
                if agent.tool_parameters.n_flow_ids != 1:
                    raise ValueError("`n_flow_ids` must be 1 for ping and yarrp")
        return values


class MeasurementPostResponse(BaseModel):
    """POST /measurements (Response)."""

    uuid: UUID


class MeasurementDeleteResponse(BaseModel):
    """DELETE /measurements/{uuid} (Response)."""

    uuid: UUID
    action: str
