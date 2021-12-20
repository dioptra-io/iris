import re
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from pydantic import NonNegativeInt, PositiveInt, root_validator
from sqlalchemy_utils import UUIDType
from sqlmodel import Column, Field, Session, SQLModel, func, select

from iris.commons.schemas.agents import AgentParameters
from iris.commons.schemas.base import BaseModel, ListType


class Round(BaseModel):
    number: PositiveInt
    limit: NonNegativeInt
    offset: NonNegativeInt

    def __str__(self):
        return f"Round#{self.number}.{self.offset}"

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
    start_time: datetime
    end_time: datetime
    filtered_low_ttl: NonNegativeInt
    filtered_high_ttl: NonNegativeInt
    filtered_prefix_excl: NonNegativeInt
    filtered_prefix_not_incl: NonNegativeInt
    probes_read: NonNegativeInt
    packets_sent: NonNegativeInt
    packets_failed: NonNegativeInt
    packets_received: NonNegativeInt
    packets_received_invalid: NonNegativeInt
    pcap_received: NonNegativeInt
    pcap_dropped: NonNegativeInt
    pcap_interface_dropped: NonNegativeInt


class Tool(str, Enum):
    DiamondMiner = "diamond-miner"
    Yarrp = "yarrp"
    Ping = "ping"
    Probes = "probes"


class ToolParameters(BaseModel):
    initial_source_port: int = Field(
        24000, title="Initial source port", gt=0, lt=65_536
    )
    destination_port: int = Field(33434, title="Destination port", gt=0, lt=65_536)
    max_round: int = Field(10, title="Maximum round", gt=0, lt=256)
    failure_rate: float = Field(
        0.05, title="Diamond-Miner failure rate", description="Ignored for other tools"
    )
    flow_mapper: str = Field(FlowMapper.RandomFlowMapper, title="Flow mapper")
    flow_mapper_kwargs: Optional[Dict[str, Any]] = Field(
        {"seed": 42}, title="Flow mapper optional arguments"
    )
    prefix_len_v4: int = Field(24, ge=0, le=32, title="Target prefix length")
    prefix_len_v6: int = Field(64, ge=0, le=128, title="Target prefix length")
    global_min_ttl: int = Field(
        0,
        ge=0,
        le=255,
        title="Global Min TTL",
        description="Do not set. Overridden by the API",
    )
    global_max_ttl: int = Field(
        255,
        ge=0,
        le=255,
        title="Global Max TTL",
        description="Do not set. Overridden by the API",
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

    uuid: UUID = Field(..., title="UUID")
    state: MeasurementState = Field(..., title="State")
    tool: Tool = Field(..., title="Probing tool")
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

    uuid: UUID = Field(..., title="UUID")
    state: MeasurementState = Field(..., title="State")
    specific: MeasurementAgentSpecific
    parameters: AgentParameters
    probing_statistics: List[ProbingStatistics]


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
                # NOTE: We could use other values, but this would require to change
                # the Diamond-Miner results schema which has a materialized column
                # for the destination prefix which assumes /24 and /64 prefixes.
                if agent.tool_parameters.prefix_len_v4 != 24:
                    raise ValueError("`prefix_len_v4` must be 24 for diamond-miner")
                if agent.tool_parameters.prefix_len_v6 != 64:
                    raise ValueError("`prefix_len_v6` must be 64 for diamond-miner")
            if tool in [tool.Ping, tool.Probes]:
                # NOTE: Technically we could use a larger prefix length to allow
                # the flow mapper to choose a random IP address inside the prefix,
                # but users probably expect ping to target a specific IP address.
                if agent.tool_parameters.prefix_len_v4 != 32:
                    raise ValueError("`prefix_len_v4` must be 32 for ping and probes")
                if agent.tool_parameters.prefix_len_v6 != 128:
                    raise ValueError("`prefix_len_v6` must be 128 for ping and probes")
        return values


class MeasurementPostResponse(BaseModel):
    """POST /measurements (Response)."""

    uuid: UUID = Field(..., title="UUID")


class MeasurementDeleteResponse(BaseModel):
    """DELETE /measurements/{uuid} (Response)."""

    uuid: UUID = Field(..., title="UUID")
    action: str


class MeasurementRequest(MeasurementPostBody):
    start_time: datetime = Field(
        default_factory=lambda: datetime.utcnow().replace(microsecond=0)
    )
    uuid: UUID = Field(default_factory=uuid4)
    user_id: UUID

    def agent(self, uuid: UUID) -> MeasurementAgentPostBody:
        for agent in self.agents:
            if agent.uuid == uuid:
                return agent
        raise ValueError(f"no agent found for UUID {uuid}")


class MeasurementRoundRequest(BaseModel):
    measurement: MeasurementRequest
    probe_filename: str
    round: Round


class Measurement(SQLModel, table=True):
    """
    >>> from iris.commons.test import create_test_engine
    >>> engine = create_test_engine()
    >>> request_1 = MeasurementRequest(
    ...     user_id=uuid4(), agents=[], tags=["public", "exhaustive"]
    ... )
    >>> request_2 = MeasurementRequest(
    ...     user_id=uuid4(), agents=[], tags=["exhaustive"]
    ... )
    >>> Measurement.register(engine, request_1)
    >>> Measurement.register(engine, request_2)
    >>> Measurement.count(engine)
    2
    >>> Measurement.count(engine, tag="public")
    1
    >>> Measurement.count(engine, user_id=request_1.user_id)
    1
    >>> Measurement.all(engine, user_id=request_1.user_id)[0].state
    MeasurementState.Ongoing
    >>> Measurement.stamp(engine, request_1.uuid, MeasurementState.Finished)
    >>> Measurement.all(engine, user_id=request_1.user_id)[0].state
    MeasurementState.Finished
    """

    # NOTE: We temporarily use UUIDType from sqlalchemy_utils until
    # https://github.com/tiangolo/sqlmodel/issues/25 is fixed.
    uuid: UUID = Field(
        default_factory=uuid4,
        sa_column=Column(UUIDType(), primary_key=True),
        title="UUID",
    )
    user_id: UUID = Field(sa_column=Column(UUIDType()))
    tool: Tool = Field(title="Probing tool")
    state: MeasurementState = Field(title="State")
    tags: List[str] = Field(sa_column=Column(ListType()))
    start_time: datetime
    end_time: Optional[datetime]
    # Not stored in the database, dynamically inserted by the API
    agents: List[MeasurementAgent] = Field(sa_column=Column(ListType()))

    @classmethod
    def all(
        cls,
        engine,
        *,
        tag: Optional[str] = None,
        user_id: Optional[UUID] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List["Measurement"]:
        with Session(engine) as session:
            query = select(Measurement).offset(offset).limit(limit)
            if tag:
                query = query.where(Measurement.tags.contains(tag))
            if user_id:
                query = query.where(Measurement.user_id == user_id)
            return session.exec(query).all()

    @classmethod
    def count(
        cls,
        engine,
        *,
        tag: Optional[str] = None,
        user_id: Optional[UUID] = None,
    ) -> int:
        with Session(engine) as session:
            query = select(func.count(Measurement.uuid))
            if tag:
                query = query.where(Measurement.tags.contains(tag))
            if user_id:
                query = query.where(Measurement.user_id == user_id)
            return session.exec(query).one()

    @classmethod
    def get(cls, engine, uuid: UUID) -> Optional["Measurement"]:
        with Session(engine) as session:
            return session.get(Measurement, uuid)

    @classmethod
    def register(cls, engine, r: MeasurementRequest) -> None:
        with Session(engine) as session:
            session.add(
                Measurement(
                    uuid=r.uuid,
                    user_id=r.user_id,
                    tool=r.tool,
                    tags=r.tags,
                    state=MeasurementState.Ongoing,
                    start_time=r.start_time,
                    end_time=None,
                )
            )
            session.commit()

    @classmethod
    def stamp(cls, engine, uuid: UUID, state: MeasurementState) -> None:
        with Session(engine) as session:
            measurement = session.get(Measurement, uuid)
            measurement.end_time = datetime.utcnow()
            measurement.state = state
            session.add(measurement)
            session.commit()
