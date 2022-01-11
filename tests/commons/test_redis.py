import asyncio
from uuid import uuid4

import pytest

from iris.commons.models.agent import Agent, AgentState
from iris.commons.models.measurement_round_request import MeasurementRoundRequest
from iris.commons.models.round import Round
from iris.commons.utils import cancel_task

pytestmark = pytest.mark.asyncio


async def test_redis_get_agents_empty(redis):
    assert len(await redis.get_agents()) == 0


async def test_redis_get_agents(redis, make_agent_parameters):
    agent_uuid_1 = str(uuid4())
    agent_uuid_2 = str(uuid4())
    agent_parameters_1 = make_agent_parameters()
    agent_parameters_2 = make_agent_parameters()

    await redis.register_agent(agent_uuid_1, 1)
    await redis.register_agent(agent_uuid_2, 1)
    await redis.set_agent_parameters(agent_uuid_1, agent_parameters_1)
    await redis.set_agent_parameters(agent_uuid_2, agent_parameters_2)

    assert len(await redis.get_agents()) == 2


async def test_redis_get_agent(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    agent_parameters = make_agent_parameters()

    await redis.register_agent(agent_uuid, 1)
    await redis.set_agent_parameters(agent_uuid, agent_parameters)
    await redis.set_agent_state(agent_uuid, AgentState.Idle)

    assert len(await redis.get_agents()) == 1
    assert len(await redis.get_agents_by_uuid()) == 1
    assert await redis.get_agent_by_uuid(agent_uuid) == Agent(
        uuid=agent_uuid, state=AgentState.Idle, parameters=agent_parameters
    )


async def test_redis_get_agent_expired(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    agent_parameters = make_agent_parameters()

    await redis.register_agent(agent_uuid, 1)
    await redis.set_agent_parameters(agent_uuid, agent_parameters)
    await asyncio.sleep(1)

    assert len(await redis.get_agents()) == 0


async def test_redis_get_agent_unregistered(redis):
    agent_uuid = str(uuid4())
    await redis.register_agent(agent_uuid, 1)
    await redis.unregister_agent(agent_uuid)
    assert len(await redis.get_agents()) == 0


async def test_redis_check_agent_not_registered(redis):
    assert not await redis.check_agent(uuid4())


async def test_redis_check_agent_fully_registered(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    agent_parameters = make_agent_parameters()

    await redis.register_agent(agent_uuid, 5)
    await redis.set_agent_parameters(agent_uuid, agent_parameters)
    await redis.set_agent_state(agent_uuid, AgentState.Working)
    assert await redis.check_agent(agent_uuid)


async def test_redis_check_agent_missing_state(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    agent_parameters = make_agent_parameters()

    await redis.register_agent(agent_uuid, 5)
    await redis.set_agent_parameters(agent_uuid, agent_parameters)
    assert not await redis.check_agent(agent_uuid)


async def test_redis_check_agent_missing_parameters(redis):
    agent_uuid = str(uuid4())
    await redis.register_agent(agent_uuid, 5)
    await redis.set_agent_state(agent_uuid, AgentState.Working)
    assert not await redis.check_agent(agent_uuid)


async def test_redis_set_agent_state(redis):
    agent_uuid = str(uuid4())

    await redis.set_agent_state(agent_uuid, AgentState.Working)
    assert await redis.get_agent_state(agent_uuid) == AgentState.Working

    await redis.delete_agent_state(agent_uuid)
    assert await redis.get_agent_state(agent_uuid) == AgentState.Unknown


async def test_redis_set_agent_parameters(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    agent_parameters = make_agent_parameters()

    await redis.set_agent_parameters(agent_uuid, agent_parameters)
    assert await redis.get_agent_parameters(agent_uuid) == agent_parameters

    await redis.delete_agent_parameters(agent_uuid)
    assert await redis.get_agent_parameters(agent_uuid) is None


async def test_redis_cancel_measurement_agent(redis):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())
    assert not await redis.measurement_agent_cancelled(measurement_uuid, agent_uuid)
    await redis.cancel_measurement_agent(measurement_uuid, agent_uuid)
    assert await redis.measurement_agent_cancelled(measurement_uuid, agent_uuid)


async def test_redis_set_measurement_stats(redis, make_probing_statistics):
    agent_uuid = str(uuid4())
    measurement_uuid = str(uuid4())
    statistics = make_probing_statistics()

    await redis.set_measurement_stats(measurement_uuid, agent_uuid, statistics)
    assert await redis.get_measurement_stats(measurement_uuid, agent_uuid) == statistics

    await redis.delete_measurement_stats(measurement_uuid, agent_uuid)
    assert await redis.get_measurement_stats(measurement_uuid, agent_uuid) is None


async def test_redis_publish_subscribe(redis, make_user, make_measurement):
    user = make_user()
    measurement = make_measurement(user_id=str(user.id))
    measurement_agent = measurement.agents[0]

    # 1. Create a queue
    queue = asyncio.Queue()

    # 2. Launch the subscriber and give it some time to start
    subscriber = asyncio.create_task(
        redis.subscribe(measurement_agent.agent_uuid, queue)
    )
    await asyncio.sleep(0.5)

    # 3. Publish the request
    request = MeasurementRoundRequest(
        measurement=measurement,
        measurement_agent=measurement_agent,
        probe_filename="",
        round=Round(number=1, limit=10, offset=0),
    )
    await redis.publish(measurement_agent.agent_uuid, request)

    # 3. Verify that the request is in the queue
    result = await asyncio.wait_for(queue.get(), timeout=0.5)
    assert result == request

    # 4. Terminate the subscriber
    await cancel_task(subscriber)
