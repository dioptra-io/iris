from datetime import datetime
from typing import List, Optional
from uuid import uuid4

from pydantic import root_validator
from sqlalchemy import desc
from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import Column, Enum, Field, Relationship, Session, String, func, select

from iris.commons.models.base import BaseSQLModel
from iris.commons.models.diamond_miner import Tool
from iris.commons.models.measurement_agent import (
    MeasurementAgent,
    MeasurementAgentCreate,
    MeasurementAgentRead,
    MeasurementAgentState,
)
from iris.commons.utils import cast


class MeasurementBase(BaseSQLModel):
    tool: Tool = Field(sa_column=Column(Enum(Tool, native_enum=False)), title="Tool")
    tags: List[str] = Field(
        default_factory=list, sa_column=Column(ARRAY(String)), title="Tags"
    )


class MeasurementCreate(MeasurementBase):
    agents: List[MeasurementAgentCreate] = Field(
        title="Agents participating to the measurement",
        description="Optional agent parameters can also be set",
    )

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
    creation_time: datetime
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    state: MeasurementAgentState = Field(title="State")

    @classmethod
    def from_measurement(cls, m: "Measurement") -> "MeasurementRead":
        # TODO: Try to get rid of this by having Pydantic/SQLModel take into account
        # computed properties (@property).
        return cast(cls, m, start_time=m.start_time, end_time=m.end_time, state=m.state)

    @classmethod
    def from_measurements(cls, ms: List["Measurement"]) -> List["MeasurementRead"]:
        return [cls.from_measurement(m) for m in ms]


class MeasurementPatch(BaseSQLModel):
    tags: List[str] = Field(default_factory=list, title="Tags")


class MeasurementReadWithAgents(MeasurementRead):
    agents: List[MeasurementAgentRead]


class Measurement(MeasurementBase, table=True):
    uuid: str = Field(default_factory=lambda: str(uuid4()), primary_key=True)
    creation_time: datetime = Field(default_factory=lambda: datetime.utcnow())
    user_id: str
    agents: List[MeasurementAgent] = Relationship(back_populates="measurement")

    @classmethod
    def all(
        cls,
        session: Session,
        *,
        state: Optional[MeasurementAgentState] = None,
        tags: List[str] = None,
        user_id: Optional[str] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List["Measurement"]:
        query = (
            select(Measurement)
            .offset(offset)
            .limit(limit)
            .order_by(desc(Measurement.creation_time))
        )
        if state:
            query = query.where(Measurement.agents.any(state=state))
        if tags:
            query = query.where(Measurement.tags.contains(tags))
        if user_id:
            query = query.where(Measurement.user_id == user_id)
        return session.exec(query).all()

    @classmethod
    def count(
        cls,
        session: Session,
        *,
        state: Optional[MeasurementAgentState] = None,
        tags: List[str] = None,
        user_id: Optional[str] = None,
    ) -> int:
        query = select(func.count(Measurement.uuid))  # type: ignore
        if state:
            query = query.where(Measurement.agents.any(state=state))
        if tags:
            query = query.where(Measurement.tags.contains(tags))
        if user_id:
            query = query.where(Measurement.user_id == user_id)
        return int(session.exec(query).one())

    @classmethod
    def get(cls, session: Session, uuid: str) -> Optional["Measurement"]:
        return session.get(Measurement, uuid)

    @property
    def start_time(self):
        try:
            return min(x.start_time for x in self.agents)
        except (TypeError, ValueError):
            return None

    @property
    def end_time(self):
        try:
            return max(x.end_time for x in self.agents)
        except (TypeError, ValueError):
            return None

    @property
    def state(self):
        agents_state = {x.state for x in self.agents}
        if len(agents_state) == 1:
            return agents_state.pop()
        return MeasurementAgentState.Ongoing

    def set_tags(self, session: Session, tags: List[str]) -> None:
        self.tags = tags
        session.add(self)
        session.commit()
