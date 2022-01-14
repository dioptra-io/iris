from uuid import uuid4

import pytest

from iris.commons.models import (
    AgentState,
    MeasurementAgentCreate,
    MeasurementAgentState,
    MeasurementCreate,
    Tool,
)
from iris.commons.models.status import Status
from tests.assertions import cast_response
from tests.helpers import create_user_buckets, register_agent, upload_target_file

pytestmark = pytest.mark.asyncio


async def test_get_status(
    make_client, make_user, make_agent_parameters, redis, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    # Register an agent
    agent_uuid = str(uuid4())
    await register_agent(redis, agent_uuid, make_agent_parameters(), AgentState.Idle)

    # Create a measurement
    await create_user_buckets(storage, user)
    await upload_target_file(storage, user, "targets.csv", ["0.0.0.0/0,icmp,8,32,6"])
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentCreate(uuid=agent_uuid, target_file="targets.csv")],
    )
    client.post("/measurements/", data=body.json())

    # Check status
    status = cast_response(client.get("/status"), Status)
    assert status.agents == {AgentState.Idle: 1}
    assert status.buckets >= 1
    assert status.measurements == {MeasurementAgentState.Created: 1}
