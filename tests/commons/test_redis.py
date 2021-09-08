import logging
from uuid import uuid4

import aioredis
import pytest

from iris.commons.redis import AgentRedis, Redis
from iris.commons.schemas.public import AgentState, MeasurementState


@pytest.mark.asyncio
async def test_redis_test(common_settings, redis_client):
    # Valid connection
    redis = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), uuid4()
    )
    assert await redis.test()
    await redis.disconnect()

    # Invalid connection
    redis = AgentRedis(
        aioredis.from_url("redis://127.0.0.1:6380"),
        common_settings,
        logging.getLogger(__name__),
        uuid4(),
    )
    assert not await redis.test()


@pytest.mark.asyncio
async def test_redis_get_agents(common_settings, fake_agent, redis_client):
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    agent_redis = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), fake_agent.uuid
    )

    # TODO: Ensure that all agents are disconnected beforehand?
    assert len(await redis.get_agents()) == 0
    await agent_redis.register()
    assert len(await redis.get_agents()) == 1
    assert len(await redis.get_agents_by_uuid()) == 1


@pytest.mark.asyncio
async def test_redis_check_agent(common_settings, fake_agent, redis_client):
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    agent_redis = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), fake_agent.uuid
    )

    # Not registered
    assert not await redis.check_agent(uuid4())
    await agent_redis.register()

    # Fully registered
    await agent_redis.set_agent_parameters(fake_agent.parameters)
    await agent_redis.set_agent_state(AgentState.Working)
    assert redis.check_agent(fake_agent.uuid)

    # Missing state
    await agent_redis.set_agent_parameters(fake_agent.parameters)
    await agent_redis.delete_agent_state()
    assert not await redis.check_agent(fake_agent.uuid)

    # Missing parameters
    await agent_redis.delete_agent_parameters()
    await agent_redis.set_agent_state(AgentState.Working)
    assert not await redis.check_agent(fake_agent.uuid)


@pytest.mark.asyncio
async def test_redis_agent_state(common_settings, fake_agent, redis_client):
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    agent_redis = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), fake_agent.uuid
    )

    await agent_redis.set_agent_state(AgentState.Working)
    assert await redis.get_agent_state(fake_agent.uuid) == AgentState.Working
    await agent_redis.delete_agent_state()
    assert await redis.get_agent_state(fake_agent.uuid) == AgentState.Unknown


@pytest.mark.asyncio
async def test_redis_agent_parameters(common_settings, fake_agent, redis_client):
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    agent_redis = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), fake_agent.uuid
    )

    assert await redis.get_agent_parameters(fake_agent.uuid) is None
    await agent_redis.set_agent_parameters(fake_agent.parameters)
    assert await redis.get_agent_parameters(fake_agent.uuid) == fake_agent.parameters
    await agent_redis.delete_agent_parameters()
    assert await redis.get_agent_parameters(fake_agent.uuid) is None


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
async def test_redis_measurement_stats(common_settings, fake_agent, redis_client):
    measurement_uuid = uuid4()
    redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
    assert await redis.get_measurement_stats(measurement_uuid, fake_agent.uuid) == {}
    await redis.set_measurement_stats(measurement_uuid, fake_agent.uuid, {"foo": "bar"})
    assert await redis.get_measurement_stats(measurement_uuid, fake_agent.uuid) == {
        "foo": "bar"
    }
    await redis.delete_measurement_stats(measurement_uuid, fake_agent.uuid)
    assert await redis.get_measurement_stats(measurement_uuid, fake_agent.uuid) == {}


# @pytest.mark.asyncio
# async def test_redis_pubsub(common_settings, redis_client):
#     fake_agent.uuid = uuid4()
#
#     redis = Redis(redis_client, common_settings, logging.getLogger(__name__))
#     agent_redis = AgentRedis(
#         redis_client, common_settings, logging.getLogger(__name__), fake_agent.uuid
#     )
#
#     await redis.publish(f"{agent_redis.KEY_AGENT_LISTEN}:all", {"foo": "bar"})
#     assert await agent_redis.subscribe() == {"foo": "bar"}
