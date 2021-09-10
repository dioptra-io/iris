from uuid import uuid4

import pytest

from iris.commons.database import Agents
from iris.commons.schemas.private import MeasurementRequest
from iris.commons.schemas.public import MeasurementAgentPostBody, MeasurementState


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

    formatted = {
        "uuid": str(measurement_agent.uuid),
        "target_file": measurement_agent.target_file,
        "probing_rate": measurement_agent.probing_rate,
        "probing_statistics": {},
        "agent_parameters": agent.parameters,
        "tool_parameters": measurement_agent.tool_parameters,
        "state": MeasurementState.Ongoing,
    }

    assert (
        await db.register(measurement_request, measurement_agent.uuid, agent.parameters)
        is None
    )
    assert await db.get(uuid4(), uuid4()) is None
    assert await db.all(measurement_uuid=uuid4()) == []
    assert await db.all(measurement_uuid=measurement_request.uuid) == [formatted]
    assert (
        await db.get(
            measurement_uuid=measurement_request.uuid, agent_uuid=measurement_agent.uuid
        )
        == formatted
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
    )["probing_statistics"] == {"1:0:0": {"packets_sent": 10}}

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
    )["probing_statistics"] == {
        "1:0:0": {"packets_sent": 10},
        "2:0:0": {"packets_sent": 30},
    }

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
    )["state"] == MeasurementState.Canceled

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
    )["state"] == MeasurementState.Finished
