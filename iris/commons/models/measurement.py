from datetime import datetime
from typing import List, Optional
from uuid import uuid4

from pydantic import root_validator
from sqlalchemy import Enum
from sqlmodel import Column, Field, Relationship, Session, func, select

from iris.commons.models.base import BaseSQLModel, ListType
from iris.commons.models.diamond_miner import Tool
from iris.commons.models.measurement_agent import (
    MeasurementAgent,
    MeasurementAgentCreate,
    MeasurementAgentRead,
    MeasurementAgentState,
)
from iris.commons.utils import cast


class MeasurementBase(BaseSQLModel):
    tool: Tool = Field(sa_column=Column(Enum(Tool)), title="Probing tool")
    tags: List[str] = Field(
        default_factory=list, sa_column=Column(ListType()), title="Tags"
    )


class MeasurementCreate(MeasurementBase):
    agents: List[MeasurementAgentCreate] = Field(
        title="Agents participating to the measurement",
        description="Optional agent parameters can also be set",
    )

    # TODO: Call this!
    @root_validator
    def check_tool_parameters(cls, values):
        agents: List[MeasurementAgentCreate] = values.get("agents")
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


class MeasurementRead(MeasurementBase):
    uuid: str = Field(title="UUID")
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    state: MeasurementAgentState = Field(title="State")

    @classmethod
    def from_measurement(cls, m: "Measurement") -> "MeasurementRead":
        try:
            start_time = min(x.start_time for x in m.agents)
        except TypeError:
            start_time = None
        try:
            end_time = min(x.end_time for x in m.agents)
        except TypeError:
            end_time = None
        agents_state = {x.state for x in m.agents}
        if len(agents_state) == 1:
            state = agents_state.pop()
        else:
            state = MeasurementAgentState.Ongoing
        return cast(cls, m, start_time=start_time, end_time=end_time, state=state)

    @classmethod
    def from_measurements(cls, ms: List["Measurement"]) -> List["MeasurementRead"]:
        return [cls.from_measurement(m) for m in ms]


class MeasurementReadWithAgents(MeasurementRead):
    agents: List[MeasurementAgentRead]


class Measurement(MeasurementBase, table=True):
    uuid: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    user_id: str
    agents: List[MeasurementAgent] = Relationship(back_populates="measurement")

    @classmethod
    def all(
        cls,
        session: Session,
        *,
        tag: Optional[str] = None,
        user_id: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List["Measurement"]:
        query = select(Measurement).offset(offset).limit(limit)
        if tag:
            query = query.where(Measurement.tags.contains(tag))
        if user_id:
            query = query.where(Measurement.user_id == user_id)
        return session.exec(query).all()

    @classmethod
    def count(
        cls,
        session: Session,
        *,
        tag: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> int:
        query = select(func.count(Measurement.uuid))  # type: ignore
        if tag:
            query = query.where(Measurement.tags.contains(tag))
        if user_id:
            query = query.where(Measurement.user_id == user_id)
        return session.exec(query).one()

    @classmethod
    def get(cls, session: Session, uuid: str) -> Optional["Measurement"]:
        return session.get(Measurement, uuid)
