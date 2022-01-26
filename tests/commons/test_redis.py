import asyncio
from uuid import uuid4

import pytest

from iris.commons.models.agent import Agent, AgentState
from iris.commons.models.measurement_round_request import MeasurementRoundRequest
from iris.commons.models.round import Round


async def test_get_agents_empty(redis):
    assert len(await redis.get_agents()) == 0


async def test_get_agents(redis, make_agent_parameters):
    agent_uuid_1 = str(uuid4())
    agent_uuid_2 = str(uuid4())
    agent_parameters_1 = make_agent_parameters()
    agent_parameters_2 = make_agent_parameters()

    await redis.register_agent(agent_uuid_1, 1)
    await redis.register_agent(agent_uuid_2, 1)
    await redis.set_agent_parameters(agent_uuid_1, agent_parameters_1)
    await redis.set_agent_parameters(agent_uuid_2, agent_parameters_2)

    assert len(await redis.get_agents()) == 2


async def test_get_agent(redis, make_agent_parameters):
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


async def test_get_agent_expired(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    agent_parameters = make_agent_parameters()

    await redis.register_agent(agent_uuid, 1)
    await redis.set_agent_parameters(agent_uuid, agent_parameters)
    await asyncio.sleep(1)

    assert len(await redis.get_agents()) == 0


async def test_get_agent_unregistered(redis):
    agent_uuid = str(uuid4())
    await redis.register_agent(agent_uuid, 1)
    await redis.unregister_agent(agent_uuid)
    assert len(await redis.get_agents()) == 0


async def test_check_agent_not_registered(redis):
    assert not await redis.check_agent(uuid4())


async def test_check_agent_fully_registered(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    agent_parameters = make_agent_parameters()

    await redis.register_agent(agent_uuid, 5)
    await redis.set_agent_parameters(agent_uuid, agent_parameters)
    await redis.set_agent_state(agent_uuid, AgentState.Working)
    assert await redis.check_agent(agent_uuid)


async def test_check_agent_missing_state(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    agent_parameters = make_agent_parameters()

    await redis.register_agent(agent_uuid, 5)
    await redis.set_agent_parameters(agent_uuid, agent_parameters)
    assert not await redis.check_agent(agent_uuid)


async def test_check_agent_missing_parameters(redis):
    agent_uuid = str(uuid4())
    await redis.register_agent(agent_uuid, 5)
    await redis.set_agent_state(agent_uuid, AgentState.Working)
    assert not await redis.check_agent(agent_uuid)


async def test_set_agent_state(redis):
    agent_uuid = str(uuid4())

    await redis.set_agent_state(agent_uuid, AgentState.Working)
    assert await redis.get_agent_state(agent_uuid) == AgentState.Working

    await redis.delete_agent_state(agent_uuid)
    assert await redis.get_agent_state(agent_uuid) == AgentState.Unknown


async def test_set_agent_parameters(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    agent_parameters = make_agent_parameters()

    await redis.set_agent_parameters(agent_uuid, agent_parameters)
    assert await redis.get_agent_parameters(agent_uuid) == agent_parameters

    await redis.delete_agent_parameters(agent_uuid)
    assert await redis.get_agent_parameters(agent_uuid) is None


async def test_set_measurement_stats(redis, make_probing_statistics):
    agent_uuid = str(uuid4())
    measurement_uuid = str(uuid4())
    statistics = make_probing_statistics()

    await redis.set_measurement_stats(measurement_uuid, agent_uuid, statistics)
    assert await redis.get_measurement_stats(measurement_uuid, agent_uuid) == statistics

    await redis.delete_measurement_stats(measurement_uuid, agent_uuid)
    assert await redis.get_measurement_stats(measurement_uuid, agent_uuid) is None


async def test_get_random_request_empty(redis):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            redis.get_random_request(str(uuid4()), interval=0.1), 0.5
        )


async def test_get_random_request(redis):
    agent_uuid = str(uuid4())
    request_1 = MeasurementRoundRequest(
        measurement_uuid=str(uuid4()),
        probe_filename="request_1",
        probing_rate=100,
        round=Round(number=1, limit=10, offset=0),
    )
    request_2 = MeasurementRoundRequest(
        measurement_uuid=str(uuid4()),
        probe_filename="request_1",
        probing_rate=100,
        round=Round(number=1, limit=10, offset=0),
    )

    await redis.set_request(agent_uuid, request_1)
    await redis.set_request(agent_uuid, request_2)
    assert await redis.get_random_request(agent_uuid) in (request_1, request_2)

    await redis.delete_request(request_1.measurement_uuid, agent_uuid)
    assert await redis.get_random_request(agent_uuid) == request_2

    await redis.delete_request(request_2.measurement_uuid, agent_uuid)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(redis.get_random_request(agent_uuid, interval=0.1), 0.5)
