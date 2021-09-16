import asyncio
import logging
from uuid import uuid4

import pytest

from iris.commons.redis import AgentRedis, Redis
from iris.commons.schemas.public import AgentState, MeasurementState


@pytest.mark.asyncio
async def test_redis_get_agents(common_settings, agent, redis_client):
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    agent_redis_1 = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), agent.uuid
    )
    agent_redis_2 = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), uuid4()
    )

    # No agents
    assert len(await redis.get_agents()) == 0

    # Registered for 1 second
    await agent_redis_1.register(1)
    assert len(await redis.get_agents()) == 1
    assert len(await redis.get_agents_by_uuid()) == 1

    # Registration expired
    await asyncio.sleep(1)
    assert len(await redis.get_agents()) == 0

    # Register/Deregister
    await agent_redis_1.register(1)
    await agent_redis_1.deregister()
    assert len(await redis.get_agents()) == 0

    # Two agents
    await agent_redis_1.register(1)
    await agent_redis_2.register(1)
    assert len(await redis.get_agents()) == 2


@pytest.mark.asyncio
async def test_redis_check_agent(common_settings, agent, redis_client):
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    agent_redis = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), agent.uuid
    )

    # Not registered
    assert not await redis.check_agent(uuid4())
    await agent_redis.register(5)

    # Fully registered
    await agent_redis.set_agent_parameters(agent.parameters)
    await agent_redis.set_agent_state(AgentState.Working)
    assert await redis.check_agent(agent.uuid)

    # Missing state
    await agent_redis.set_agent_parameters(agent.parameters)
    await agent_redis.delete_agent_state()
    assert not await redis.check_agent(agent.uuid)

    # Missing parameters
    await agent_redis.delete_agent_parameters()
    await agent_redis.set_agent_state(AgentState.Working)
    assert not await redis.check_agent(agent.uuid)


@pytest.mark.asyncio
async def test_redis_agent_state(common_settings, agent, redis_client):
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    agent_redis = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), agent.uuid
    )

    await agent_redis.set_agent_state(AgentState.Working)
    assert await redis.get_agent_state(agent.uuid) == AgentState.Working
    await agent_redis.delete_agent_state()
    assert await redis.get_agent_state(agent.uuid) == AgentState.Unknown


@pytest.mark.asyncio
async def test_redis_agent_parameters(common_settings, agent, redis_client):
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    agent_redis = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), agent.uuid
    )

    assert await redis.get_agent_parameters(agent.uuid) is None
    await agent_redis.set_agent_parameters(agent.parameters)
    assert await redis.get_agent_parameters(agent.uuid) == agent.parameters
    await agent_redis.delete_agent_parameters()
    assert await redis.get_agent_parameters(agent.uuid) is None


@pytest.mark.asyncio
async def test_redis_measurement_state(common_settings, redis_client):
    measurement_uuid = uuid4()
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    assert (
        await redis.get_measurement_state(measurement_uuid) == MeasurementState.Unknown
    )
    await redis.set_measurement_state(measurement_uuid, MeasurementState.Ongoing)
    assert (
        await redis.get_measurement_state(measurement_uuid) == MeasurementState.Ongoing
    )
    await redis.delete_measurement_state(measurement_uuid)
    assert (
        await redis.get_measurement_state(measurement_uuid) == MeasurementState.Unknown
    )


@pytest.mark.asyncio
async def test_redis_measurement_stats(
    common_settings, agent, statistics, redis_client
):
    measurement_uuid = uuid4()
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    assert await redis.get_measurement_stats(measurement_uuid, agent.uuid) is None
    await redis.set_measurement_stats(measurement_uuid, agent.uuid, statistics)
    assert await redis.get_measurement_stats(measurement_uuid, agent.uuid) == statistics
    await redis.delete_measurement_stats(measurement_uuid, agent.uuid)
    assert await redis.get_measurement_stats(measurement_uuid, agent.uuid) is None


# @pytest.mark.asyncio
# async def test_redis_pubsub(common_settings, redis_client):
#     agent.uuid = uuid4()
#
#     redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
#     agent_redis = AgentRedis(
#         redis_client, common_settings, logging.getLogger(__name__), agent.uuid
#     )
#
#     await redis.publish(f"{agent_redis.KEY_AGENT_LISTEN}:all", {"foo": "bar"})
#     assert await agent_redis.subscribe() == {"foo": "bar"}
