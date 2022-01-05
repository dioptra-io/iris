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
from iris.worker.watch import watch_measurement_agent, watch_measurement_agent_
from tests.api.test_measurements import upload_target_file
from tests.assertions import APIResponseError, assert_status_code, cast_response
from tests.helpers import superuser

pytestmark = pytest.mark.asyncio


# TODO: Move to helpers
async def cancel_task(task: Task):
    task.cancel()
    try:
        await task
    except CancelledError:
        pass


async def test_agent_registration(agent_settings, make_client, make_user):
    agent_task = asyncio.create_task(iris.agent.main.main(agent_settings))
    client = make_client(make_user(probing_enabled=True))
    for _ in range(3):
        try:
            agent = cast_response(
                client.get(f"/agents/{agent_settings.AGENT_UUID}"), Agent
            )
            assert agent.uuid == agent_settings.AGENT_UUID
            assert agent.state == AgentState.Idle
            assert agent.parameters.version == __version__
            break
        except APIResponseError:
            await asyncio.sleep(0.5)
    else:
        pytest.fail("agent not found")
    await cancel_task(agent_task)


async def test_e2e(
    make_client,
    make_user,
    agent_settings,
    worker_settings,
    stub_broker,
    stub_worker,
    storage,
):
    # Configure the worker
    iris.worker.watch.settings = worker_settings

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

    stub_broker.join(watch_measurement_agent.queue_name)
    stub_worker.join()

    await cancel_task(agent_task)

    # TODO: Mock send
    # TODO: How to pass the settings to the agent?
    #
    # user = make_user(probing_enabled=True)
    # client = make_client(user)
    # agent_redis = make_agent_redis(str(uuid4()))
    # # TODO: The agent should register itself, make calls to /agents
    # # and wait for it to appear (with a timeout).
    # await agent_redis.register(5)
    # await agent_redis.set_agent_parameters(make_agent_parameters())
    # await agent_redis.set_agent_state(AgentState.Idle)
    # body = MeasurementCreate(
    #     tool=Tool.DiamondMiner,
    #     agents=[
    #         MeasurementAgentCreate(uuid=agent_redis.uuid, target_file="targets.csv")
    #     ],
    # )
    # # TODO: How to handle target file archiving (in the API) with one bucket per (measurement, agent)?
    # await upload_target_file(storage, user, "targets.csv", ["0.0.0.0/0,icmp,8,32,6"])
    # # TODO: Best place to create the test buckets?
    # await storage.create_bucket(storage.archive_bucket(str(user.id)))
    # response = client.post("/measurements/", data=body.json())
    # measurement = cast_response(response, MeasurementReadWithAgents)
    #
    # # agent_task = asyncio.create_task(iris.agent.main.main())
    # # TODO: Watch multiple measurement agents?
    # # TODO: Dramatiq test broker?
    # # TODO: Proper worker settings with WORKER_RESULTS_DIR_PATH
    # worker_task = asyncio.create_task(
    #     watch_measurement_agent_(
    #         measurement_uuid=measurement.uuid,
    #         agent_uuid=agent_redis.uuid,
    #         settings=worker_settings,
    #     )
    # )
    # await asyncio.gather(worker_task)
