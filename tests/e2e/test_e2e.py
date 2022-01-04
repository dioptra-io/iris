import asyncio
from asyncio import CancelledError, Task
from uuid import uuid4

import pytest

import iris.agent.main
from iris import __version__
from iris.commons.models.agent import Agent, AgentState
from iris.commons.models.diamond_miner import Tool
from iris.commons.models.measurement import MeasurementCreate, MeasurementReadWithAgents
from iris.commons.models.measurement_agent import MeasurementAgentCreate
from iris.commons.models.pagination import Paginated
from iris.worker.watch import watch_measurement_agent_
from tests.api.test_measurements import upload_target_file
from tests.assertions import cast_response
from tests.helpers import superuser

pytestmark = pytest.mark.asyncio


# TODO: Move to helpers
async def cancel_task(task: Task):
    task.cancel()
    try:
        await task
    except CancelledError:
        pass


@superuser
async def test_agent_registration(agent_settings, make_client, make_user):
    agent_task = asyncio.create_task(iris.agent.main.main(agent_settings))
    client = make_client(make_user(probing_enabled=True))
    for _ in range(3):
        agents = cast_response(client.get("/agents"), Paginated[Agent])
        if agents.count == 1:
            assert agents.results[0].uuid == agent_settings.AGENT_UUID
            assert agents.results[0].state == AgentState.Idle
            assert agents.results[0].parameters.version == __version__
            break
        await asyncio.sleep(0.5)
    else:
        pytest.fail("agent not found")
    await cancel_task(agent_task)


@superuser
async def test_e2e(
    settings,
    make_client,
    make_user,
    make_agent_parameters,
    make_agent_redis,
    storage,
    worker_settings,
):
    # TODO: Mock send
    # TODO: How to pass the settings to the agent?

    user = make_user(probing_enabled=True)
    client = make_client(user)
    agent_redis = make_agent_redis(str(uuid4()))
    # TODO: The agent should register itself, make calls to /agents
    # and wait for it to appear (with a timeout).
    await agent_redis.register(5)
    await agent_redis.set_agent_parameters(make_agent_parameters())
    await agent_redis.set_agent_state(AgentState.Idle)
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        agents=[
            MeasurementAgentCreate(uuid=agent_redis.uuid, target_file="targets.csv")
        ],
    )
    # TODO: How to handle target file archiving (in the API) with one bucket per (measurement, agent)?
    await upload_target_file(storage, user, "targets.csv", ["0.0.0.0/0,icmp,8,32,6"])
    # TODO: Best place to create the test buckets?
    await storage.create_bucket(storage.archive_bucket(str(user.id)))
    response = client.post("/measurements/", data=body.json())
    measurement = cast_response(response, MeasurementReadWithAgents)

    # agent_task = asyncio.create_task(iris.agent.main.main())
    # TODO: Watch multiple measurement agents?
    # TODO: Dramatiq test broker?
    # TODO: Proper worker settings with WORKER_RESULTS_DIR_PATH
    worker_task = asyncio.create_task(
        watch_measurement_agent_(
            measurement_uuid=measurement.uuid,
            agent_uuid=agent_redis.uuid,
            settings=worker_settings,
        )
    )
    await asyncio.gather(worker_task)
