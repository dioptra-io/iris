from uuid import uuid4

import pytest

from iris.commons.database import agents
from iris.commons.schemas.private import MeasurementRequest
from iris.commons.schemas.public import (
    MeasurementAgent,
    MeasurementAgentPostBody,
    MeasurementAgentSpecific,
    MeasurementState,
    Round,
)


@pytest.mark.asyncio
async def test_agents(database, agent, statistics):
    assert await agents.create_table(database, drop=True) is None

    statistics2 = statistics.copy(
        update={"round": Round(number=2, limit=0, offset=0), "packets_sent": 30}
    )

    measurement_agent = MeasurementAgentPostBody(
        uuid=uuid4(),
        target_file="custom.csv",
        probing_rate=200,
    )
    measurement_request = MeasurementRequest(
        agents=[measurement_agent], user_id=uuid4()
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
        await agents.register(
            database, measurement_request, measurement_agent.uuid, agent.parameters
        )
        is None
    )
    assert await agents.get(database, uuid4(), uuid4()) is None
    assert await agents.all(database, measurement_uuid=uuid4()) == []
    assert await agents.all(database, measurement_uuid=measurement_request.uuid) == [
        expected
    ]
    assert (
        await agents.get(
            database,
            measurement_uuid=measurement_request.uuid,
            agent_uuid=measurement_agent.uuid,
        )
        == expected
    )

    assert (
        await agents.store_probing_statistics(
            database, measurement_request.uuid, measurement_agent.uuid, statistics
        )
        is None
    )

    assert (
        await agents.get(
            database,
            measurement_uuid=measurement_request.uuid,
            agent_uuid=measurement_agent.uuid,
        )
    ).probing_statistics == [statistics]

    assert (
        await agents.store_probing_statistics(
            database, measurement_request.uuid, measurement_agent.uuid, statistics2
        )
        is None
    )

    assert (
        await agents.get(
            database,
            measurement_uuid=measurement_request.uuid,
            agent_uuid=measurement_agent.uuid,
        )
    ).probing_statistics == [statistics, statistics2]

    assert (
        await agents.stamp_canceled(
            database,
            measurement_uuid=measurement_request.uuid,
            agent_uuid=measurement_agent.uuid,
        )
        is None
    )
    assert (
        await agents.get(
            database,
            measurement_uuid=measurement_request.uuid,
            agent_uuid=measurement_agent.uuid,
        )
    ).state == MeasurementState.Canceled

    assert (
        await agents.stamp_finished(
            database,
            measurement_uuid=measurement_request.uuid,
            agent_uuid=measurement_agent.uuid,
        )
        is None
    )
    assert (
        await agents.get(
            database,
            measurement_uuid=measurement_request.uuid,
            agent_uuid=measurement_agent.uuid,
        )
    ).state == MeasurementState.Finished
