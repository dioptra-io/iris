"""
The Agents database stores the status of each agents and their measurements.
"""
import json
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from sqlmodel import Session, select

from iris.commons.database.database import Database
from iris.commons.schemas.agents import AgentParameters
from iris.commons.schemas.agents2 import AgentDatabase
from iris.commons.schemas.measurements import (
    MeasurementAgent,
    MeasurementAgentSpecific,
    MeasurementRequest,
    MeasurementState,
    ProbingStatistics,
    ToolParameters,
)


# TODO: Merge AgentDatabase with MeasurementAgent and get rid of the formatter?
def formatter2(agent: AgentDatabase) -> MeasurementAgent:
    return MeasurementAgent(
        uuid=agent.agent_uuid,
        state=agent.state,
        specific=MeasurementAgentSpecific(
            target_file=agent.target_file,
            target_file_content=[],
            probing_rate=agent.probing_rate,
            tool_parameters=ToolParameters.parse_raw(agent.tool_parameters),
        ),
        parameters=AgentParameters.parse_raw(agent.agent_parameters),
        probing_statistics=[
            ProbingStatistics(**x) for x in json.loads(agent.probing_statistics)
        ],
    )


async def all(database: Database, measurement_uuid: UUID) -> List[MeasurementAgent]:
    """Get all measurement information."""
    with Session(database.settings.sqlalchemy_engine()) as session:
        measurement_agents = session.exec(
            select(AgentDatabase).where(
                AgentDatabase.measurement_uuid == measurement_uuid
            )
        ).all()
        return [
            formatter2(measurement_agent) for measurement_agent in measurement_agents
        ]


async def get(
    database: Database, measurement_uuid: UUID, agent_uuid: UUID
) -> Optional[MeasurementAgent]:
    """Get measurement information about an agent."""
    with Session(database.settings.sqlalchemy_engine()) as session:
        measurement_agent = session.get(AgentDatabase, (measurement_uuid, agent_uuid))
    if measurement_agent:
        return formatter2(measurement_agent)
    return None


async def register(
    database: Database,
    measurement_request: MeasurementRequest,
    agent_uuid: UUID,
    agent_parameters: AgentParameters,
) -> None:
    agent = measurement_request.agent(agent_uuid)
    measurement_agent = AgentDatabase(
        measurement_uuid=measurement_request.uuid,
        agent_uuid=agent_uuid,
        target_file=agent.target_file,
        probing_rate=agent.probing_rate,
        probing_statistics=json.dumps([]),
        agent_parameters=agent_parameters.json(),
        tool_parameters=agent.tool_parameters.json(),
        state=MeasurementState.Ongoing,
        timestamp=datetime.utcnow(),
    )
    with Session(database.settings.sqlalchemy_engine()) as session:
        session.add(measurement_agent)
        session.commit()


async def store_probing_statistics(
    database: Database,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    probing_statistics: ProbingStatistics,
) -> None:
    with Session(database.settings.sqlalchemy_engine()) as session:
        measurement_agent = session.get(AgentDatabase, (measurement_uuid, agent_uuid))
        # TODO: Cleanup this after model merge.
        ps = [
            ProbingStatistics(**x)
            for x in json.loads(measurement_agent.probing_statistics)
        ]
        current_probing_statistics = {x.round.encode(): x for x in ps}
        current_probing_statistics[
            probing_statistics.round.encode()
        ] = probing_statistics
        measurement_agent.probing_statistics = json.dumps(
            [json.loads(x.json()) for x in current_probing_statistics.values()]
        )
        session.add(measurement_agent)
        session.commit()


async def set_state(
    database: Database,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    state: MeasurementState,
) -> None:
    with Session(database.settings.sqlalchemy_engine()) as session:
        measurement_agent = session.get(AgentDatabase, (measurement_uuid, agent_uuid))
        measurement_agent.state = state
        session.add(measurement_agent)
        session.commit()
