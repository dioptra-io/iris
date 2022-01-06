import asyncio
from asyncio import CancelledError, Task

import pytest

import iris.agent.main
from iris.commons.models.agent import Agent
from iris.commons.models.diamond_miner import Tool
from iris.commons.models.measurement import MeasurementCreate, MeasurementReadWithAgents
from iris.commons.models.measurement_agent import MeasurementAgentCreate
from iris.worker.watch import watch_measurement_agent_
from tests.api.test_measurements import upload_target_file
from tests.assertions import APIResponseError, assert_status_code, cast_response
from tests.helpers import superuser

pytestmark = pytest.mark.asyncio


async def cancel_task(task: Task):
    task.cancel()
    try:
        await task
    except CancelledError:
        pass


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
    await upload_target_file(storage, user, "targets.csv", ["1.0.0.0/24,icmp,8,32,6"])
    # TODO: Best place to create the test buckets?
    await storage.create_bucket(storage.archive_bucket(str(user.id)))
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 201)
    measurement = cast_response(response, MeasurementReadWithAgents)

    # TODO: Explain why we do this instead of using dramatiq.
    tasks = [
        asyncio.create_task(
            watch_measurement_agent_(
                measurement.uuid, agent.agent_uuid, worker_settings
            )
        )
        for agent in measurement.agents
    ]
    await asyncio.gather(*tasks)
    await cancel_task(agent_task)

    # TODO: Call again the API and check the measurement state.
