import enum
import json
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from pydantic import root_validator
from sqlalchemy import Column, Enum
from sqlmodel import Field, Relationship, Session

from iris.commons.models.agent import AgentParameters
from iris.commons.models.base import BaseSQLModel, PydanticType
from iris.commons.models.diamond_miner import ProbingStatistics, ToolParameters

if TYPE_CHECKING:
    from iris.commons.models.measurement import Measurement


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
        sa_column=Column(PydanticType(ToolParameters)),
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
    probing_statistics: str
    state: MeasurementAgentState


class MeasurementAgent(MeasurementAgentBase, table=True):
    __tablename__ = "measurement_agent"
    measurement: "Measurement" = Relationship(back_populates="agents")
    # This is optional so that we can create a MeasurementAgent without
    # specifying the measurement_uuid and let SQLModel do it for us.
    measurement_uuid: Optional[str] = Field(
        default=None,
        primary_key=True,
        foreign_key="measurement.uuid",
    )
    agent_uuid: str = Field(primary_key=True)
    agent_parameters: AgentParameters = Field(
        sa_column=Column(PydanticType(AgentParameters))
    )
    # HACK: for some reasons SQLModel does not persist lists of Pydantic models.
    # So we set the column as str and manually encode/decode.
    # A cleaner solution would be to store them in a dedicated table
    # and add the associated relationships.
    # But this requires a composite foreign key (measurement_uuid, agent_uuid)
    # => how to do this with SQLModel?
    probing_statistics: str = Field(default="[]")
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

    def append_probing_statistics(
        self, session: Session, statistics: ProbingStatistics
    ):
        # HACK: See comment on `probing_statistics` column.
        probing_statistics = json.loads(self.probing_statistics)
        statistics_ = statistics.dict()
        statistics_["start_time"] = statistics.start_time.isoformat()
        statistics_["end_time"] = statistics.end_time.isoformat()
        probing_statistics.append(statistics_)
        self.probing_statistics = json.dumps(probing_statistics)
        session.add(self)
        session.commit()

    def set_state(self, session: Session, state: MeasurementAgentState):
        self.state = state
        session.add(self)
        session.commit()

    def set_start_time(self, session: Session, t: datetime):
        self.start_time = t
        session.add(self)
        session.commit()

    def set_end_time(self, session: Session, t: datetime):
        self.end_time = t
        session.add(self)
        session.commit()
