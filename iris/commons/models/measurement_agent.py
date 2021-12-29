import enum
from datetime import datetime
from typing import List, Optional

from pydantic import root_validator
from sqlalchemy import Column, Enum
from sqlmodel import Field, Relationship, Session

from iris.commons.models.agent import AgentParameters
from iris.commons.models.base import BaseSQLModel, JSONType
from iris.commons.models.diamond_miner import ProbingStatistics, ToolParameters


class MeasurementAgentState(enum.Enum):
    AgentFailure = "agent_failure"
    """The agent disappeared during the measurement."""
    Canceled = "canceled"
    """The measurement was canceled by the user."""
    Created = "created"
    """The measurement has been created but not yet processed by the worker."""
    Finished = "finished"
    """The measurement has finished properly."""
    Ongoing = "ongoing"
    """The measurement is watched by the worker."""


class MeasurementAgentBase(BaseSQLModel):
    tool_parameters: ToolParameters = Field(
        ToolParameters(),
        sa_column=Column(JSONType(ToolParameters)),
        title="Tool parameters",
    )
    probing_rate: Optional[int] = Field(None, title="Probing rate")
    target_file: str = Field(title="Target file key")


class MeasurementAgentCreate(MeasurementAgentBase):
    uuid: Optional[str]
    tag: Optional[str]

    @root_validator
    def check_uuid_or_tag(cls, values):
        uuid, tag = values.get("uuid"), values.get("tag")
        if bool(uuid) != bool(tag):
            return values
        raise ValueError("one of `uuid` or `tag` must be specified")


class MeasurementAgentRead(MeasurementAgentBase):
    agent_uuid: str
    agent_parameters: AgentParameters
    probing_statistics: List[ProbingStatistics]
    state: MeasurementAgentState


class MeasurementAgent(MeasurementAgentBase, table=True):
    measurement: Optional["Measurement"] = Relationship(back_populates="agents")
    measurement_uuid: Optional[str] = Field(
        default=None,
        primary_key=True,
        foreign_key="measurement.uuid",
    )
    agent_uuid: str = Field(primary_key=True)
    agent_parameters: AgentParameters = Field(
        sa_column=Column(JSONType(AgentParameters))
    )
    probing_statistics: List[ProbingStatistics] = Field(
        default_factory=list, sa_column=Column(JSONType())
    )
    start_time: Optional[datetime] = Field(default=None)
    end_time: Optional[datetime] = Field(default=None)
    state: MeasurementAgentState = Field(
        default=MeasurementAgentState.Created,
        sa_column=Column(Enum(MeasurementAgentState)),
    )

    @classmethod
    def get(
        cls, session: Session, measurement_uuid: str, agent_uuid: str
    ) -> Optional["MeasurementAgent"]:
        return session.get(MeasurementAgent, (measurement_uuid, agent_uuid))

    def set_state(self, session: Session, state: MeasurementAgentState):
        self.state = state
        session.add(self)
        session.commit()
