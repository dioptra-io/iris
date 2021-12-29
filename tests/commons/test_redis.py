import asyncio
from uuid import uuid4

import pytest

from iris.commons.models.agent import Agent, AgentState
from iris.commons.models.measurement_agent import MeasurementAgentState
from iris.commons.models.measurement_round_request import MeasurementRoundRequest
from iris.commons.models.round import Round

pytestmark = pytest.mark.asyncio


async def test_redis_get_agents_empty(redis):
    assert len(await redis.get_agents()) == 0


async def test_redis_get_agents(redis, make_agent_redis, make_agent_parameters):
    agent_redis_1 = make_agent_redis(str(uuid4()))
    agent_redis_2 = make_agent_redis(str(uuid4()))
    agent_parameters_1 = make_agent_parameters()
    agent_parameters_2 = make_agent_parameters()

    await agent_redis_1.register(1)
    await agent_redis_2.register(1)
    await agent_redis_1.set_agent_parameters(agent_parameters_1)
    await agent_redis_2.set_agent_parameters(agent_parameters_2)

    assert len(await redis.get_agents()) == 2


async def test_redis_get_agent(redis, make_agent_redis, make_agent_parameters):
    agent_redis = make_agent_redis(str(uuid4()))
    agent_parameters = make_agent_parameters()

    await agent_redis.register(1)
    await agent_redis.set_agent_parameters(agent_parameters)
    await agent_redis.set_agent_state(AgentState.Idle)

    assert len(await redis.get_agents()) == 1
    assert len(await redis.get_agents_by_uuid()) == 1
    assert await redis.get_agent_by_uuid(agent_redis.uuid) == Agent(
        uuid=agent_redis.uuid, state=AgentState.Idle, parameters=agent_parameters
    )


async def test_redis_get_agent_expired(redis, make_agent_redis, make_agent_parameters):
    agent_redis = make_agent_redis(str(uuid4()))
    agent_parameters = make_agent_parameters()

    await agent_redis.register(1)
    await agent_redis.set_agent_parameters(agent_parameters)
    await asyncio.sleep(1)

    assert len(await redis.get_agents()) == 0


async def test_redis_get_agent_unregistered(redis, make_agent_redis):
    agent_redis = make_agent_redis(str(uuid4()))
    await agent_redis.register(1)
    await agent_redis.deregister()
    assert len(await redis.get_agents()) == 0


async def test_redis_check_agent_not_registered(redis):
    assert not await redis.check_agent(uuid4())


async def test_redis_check_agent_fully_registered(
    redis, make_agent_redis, make_agent_parameters
):
    agent_redis = make_agent_redis(str(uuid4()))
    agent_parameters = make_agent_parameters()

    await agent_redis.register(5)
    await agent_redis.set_agent_parameters(agent_parameters)
    await agent_redis.set_agent_state(AgentState.Working)
    assert await redis.check_agent(agent_redis.uuid)


async def test_redis_check_agent_missing_state(
    redis, make_agent_redis, make_agent_parameters
):
    agent_redis = make_agent_redis(str(uuid4()))
    agent_parameters = make_agent_parameters()

    await agent_redis.register(5)
    await agent_redis.set_agent_parameters(agent_parameters)
    assert not await redis.check_agent(agent_redis.uuid)


async def test_redis_check_agent_missing_parameters(redis, make_agent_redis):
    agent_redis = make_agent_redis(str(uuid4()))
    await agent_redis.register(5)
    await agent_redis.set_agent_state(AgentState.Working)
    assert not await redis.check_agent(agent_redis.uuid)


async def test_redis_set_agent_state(redis, make_agent_redis):
    agent_redis = make_agent_redis(str(uuid4()))

    await agent_redis.set_agent_state(AgentState.Working)
    assert await redis.get_agent_state(agent_redis.uuid) == AgentState.Working

    await agent_redis.delete_agent_state()
    assert await redis.get_agent_state(agent_redis.uuid) == AgentState.Unknown


async def test_redis_set_agent_parameters(
    redis, make_agent_redis, make_agent_parameters
):
    agent_redis = make_agent_redis(str(uuid4()))
    agent_parameters = make_agent_parameters()

    await agent_redis.set_agent_parameters(agent_parameters)
    assert await redis.get_agent_parameters(agent_redis.uuid) == agent_parameters

    await agent_redis.delete_agent_parameters()
    assert await redis.get_agent_parameters(agent_redis.uuid) is None


async def test_redis_set_measurement_state(redis):
    measurement_uuid = str(uuid4())

    await redis.set_measurement_state(measurement_uuid, MeasurementAgentState.Ongoing)
    assert (
        await redis.get_measurement_state(measurement_uuid)
        == MeasurementAgentState.Ongoing
    )

    await redis.delete_measurement_state(measurement_uuid)
    assert (
        await redis.get_measurement_state(measurement_uuid)
        == MeasurementAgentState.Unknown
    )


async def test_redis_set_measurement_stats(redis, make_probing_statistics):
    agent_uuid = str(uuid4())
    measurement_uuid = str(uuid4())
    statistics = make_probing_statistics()

    await redis.set_measurement_stats(measurement_uuid, agent_uuid, statistics)
    assert await redis.get_measurement_stats(measurement_uuid, agent_uuid) == statistics

    await redis.delete_measurement_stats(measurement_uuid, agent_uuid)
    assert await redis.get_measurement_stats(measurement_uuid, agent_uuid) is None


async def test_redis_publish_subscribe(
    redis, make_agent_redis, make_user, make_measurement
):
    user = make_user()
    measurement = make_measurement(user_id=str(user.id))
    measurement_agent = measurement.agents[0]

    # 1. Launch the subscriber and give it some time to start
    agent_redis = make_agent_redis(measurement_agent.agent_uuid)
    subscriber = asyncio.create_task(agent_redis.subscribe())
    await asyncio.sleep(0.5)

    # 2. Publish the request
    request = MeasurementRoundRequest(
        measurement=measurement,
        measurement_agent=measurement_agent,
        probe_filename="",
        round=Round(number=1, limit=10, offset=0),
    )
    await redis.publish(measurement_agent.agent_uuid, request)

    # 3. Wait for the subscriber
    await asyncio.wait_for(subscriber, timeout=0.5)
    assert subscriber.result() == request
