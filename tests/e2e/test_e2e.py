import asyncio

import pytest

import iris.agent.main
from iris.commons.models.agent import Agent
from iris.commons.models.diamond_miner import Tool
from iris.commons.models.measurement import MeasurementCreate, MeasurementReadWithAgents
from iris.commons.models.measurement_agent import (
    MeasurementAgentCreate,
    MeasurementAgentState,
)
from iris.commons.utils import cancel_task
from iris.worker.watch import watch_measurement_agent_
from tests.assertions import APIResponseError, cast_response
from tests.helpers import create_user_buckets, superuser, upload_target_file

pytestmark = pytest.mark.asyncio


@superuser
async def test_e2e(
    agent_settings,
    worker_settings,
    make_client,
    make_user,
    storage,
):
    # Start the API
    user = make_user(probing_enabled=True)
    client = make_client(user)

    # Start the agent
    agent_task = asyncio.create_task(iris.agent.main.main(agent_settings))

    # Wait for the agent
    for _ in range(3):
        try:
            cast_response(client.get(f"/agents/{agent_settings.AGENT_UUID}"), Agent)
            break
        except APIResponseError:
            await asyncio.sleep(0.5)
    else:
        pytest.fail("agent not found")

    # Create the measurement
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        agents=[
            MeasurementAgentCreate(
                uuid=agent_settings.AGENT_UUID, target_file="targets.csv"
            )
        ],
    )
    await create_user_buckets(storage, user)
    await upload_target_file(storage, user, "targets.csv", ["1.0.0.0/24,icmp,8,32,6"])

    response = client.post("/measurements/", data=body.json())
    measurement = cast_response(response, MeasurementReadWithAgents)

    # Emulate dramatiq and launch the watchers
    tasks = [
        asyncio.create_task(
            watch_measurement_agent_(
                measurement.uuid, agent.agent_uuid, worker_settings
            )
        )
        for agent in measurement.agents
    ]

    # Wait for the watchers to complete and terminate the agent
    await asyncio.gather(*tasks)
    await cancel_task(agent_task)

    # Fetch the measurement
    response = client.get(f"/measurements/{measurement.uuid}")
    measurement = cast_response(response, MeasurementReadWithAgents)
    assert measurement.state == MeasurementAgentState.Finished
    assert measurement.start_time
    assert measurement.end_time > measurement.start_time
    assert len(measurement.agents[0].probing_statistics) > 0
