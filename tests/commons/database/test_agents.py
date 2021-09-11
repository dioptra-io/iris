from uuid import uuid4

import pytest

from iris.commons.database import Agents
from iris.commons.schemas.private import MeasurementRequest
from iris.commons.schemas.public import (
    MeasurementAgent,
    MeasurementAgentPostBody,
    MeasurementAgentSpecific,
    MeasurementState,
    ProbingStatistics,
)


@pytest.mark.asyncio
async def test_agents(database, agent):
    db = Agents(database)
    assert await db.create_table(drop=True) is None

    measurement_agent = MeasurementAgentPostBody(
        uuid=uuid4(),
        target_file="custom.csv",
        probing_rate=200,
    )
    measurement_request = MeasurementRequest(
        agents=[measurement_agent], username="test"
    )
    expected = MeasurementAgent(
        uuid=measurement_agent.uuid,
        state=MeasurementState.Ongoing,
        specific=MeasurementAgentSpecific(
            target_file="custom.csv",
            target_file_content=[],
            probing_rate=measurement_agent.probing_rate,
            tool_parameters=measurement_agent.tool_parameters,
        ),
        parameters=agent.parameters,
        probing_statistics=[],
    )

    assert (
        await db.register(measurement_request, measurement_agent.uuid, agent.parameters)
        is None
    )
    assert await db.get(uuid4(), uuid4()) is None
    assert await db.all(measurement_uuid=uuid4()) == []
    assert await db.all(measurement_uuid=measurement_request.uuid) == [expected]
    assert (
        await db.get(
            measurement_uuid=measurement_request.uuid, agent_uuid=measurement_agent.uuid
        )
        == expected
    )

    assert (
        await db.store_probing_statistics(
            measurement_request.uuid,
            measurement_agent.uuid,
            "1:0:0",
            {"packets_sent": 10},
        )
        is None
    )

    assert (
        await db.get(
            measurement_uuid=measurement_request.uuid, agent_uuid=measurement_agent.uuid
        )
    ).probing_statistics == [
        ProbingStatistics(round="1:0:0", statistics={"packets_sent": 10})
    ]

    assert (
        await db.store_probing_statistics(
            measurement_request.uuid,
            measurement_agent.uuid,
            "2:0:0",
            {"packets_sent": 30},
        )
        is None
    )

    assert (
        await db.get(
            measurement_uuid=measurement_request.uuid, agent_uuid=measurement_agent.uuid
        )
    ).probing_statistics == [
        ProbingStatistics(round="1:0:0", statistics={"packets_sent": 10}),
        ProbingStatistics(round="2:0:0", statistics={"packets_sent": 30}),
    ]

    assert (
        await db.stamp_canceled(
            measurement_uuid=measurement_request.uuid, agent_uuid=measurement_agent.uuid
        )
        is None
    )
    assert (
        await db.get(
            measurement_uuid=measurement_request.uuid, agent_uuid=measurement_agent.uuid
        )
    ).state == MeasurementState.Canceled

    assert (
        await db.stamp_finished(
            measurement_uuid=measurement_request.uuid, agent_uuid=measurement_agent.uuid
        )
        is None
    )
    assert (
        await db.get(
            measurement_uuid=measurement_request.uuid, agent_uuid=measurement_agent.uuid
        )
    ).state == MeasurementState.Finished
