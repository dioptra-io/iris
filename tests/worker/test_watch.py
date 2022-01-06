from uuid import uuid4

import pytest

from iris.commons.models.agent import AgentState
from iris.commons.models.round import Round
from iris.commons.storage import results_key
from iris.worker.watch import check_agent, clean_results, find_results
from tests.helpers import upload_file

pytestmark = pytest.mark.asyncio


async def test_check_agent_offline(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    assert not await check_agent(
        redis=redis, agent_uuid=agent_uuid, trials=3, interval=0.1
    )


async def test_check_agent_online(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    await redis.register_agent(agent_uuid, 10)
    await redis.set_agent_parameters(agent_uuid, make_agent_parameters())
    await redis.set_agent_state(agent_uuid, AgentState.Working)
    assert await check_agent(redis=redis, agent_uuid=agent_uuid, trials=3, interval=0.1)


async def test_find_results(storage, make_tmp_file):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())
    bucket = storage.measurement_agent_bucket(measurement_uuid, agent_uuid)
    await storage.create_bucket(bucket)

    tmp_filename = results_key(Round(number=1, limit=10, offset=0))
    tmp_file = make_tmp_file(tmp_filename)
    await upload_file(storage, bucket, tmp_file)

    filename = await find_results(
        storage=storage, measurement_uuid=measurement_uuid, agent_uuid=agent_uuid
    )
    assert filename == tmp_filename


async def test_find_results_not_found(storage, make_tmp_file):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())
    bucket = storage.measurement_agent_bucket(measurement_uuid, agent_uuid)
    await storage.create_bucket(bucket)
    filename = await find_results(
        storage=storage, measurement_uuid=measurement_uuid, agent_uuid=agent_uuid
    )
    assert not filename


async def test_clean_results(storage, make_tmp_file):
    measurement_uuid = str(uuid4())
    agent_uuid = str(uuid4())
    bucket = storage.measurement_agent_bucket(measurement_uuid, agent_uuid)
    await storage.create_bucket(bucket)

    tmp_filename = results_key(Round(number=1, limit=10, offset=0))
    tmp_file = make_tmp_file(tmp_filename)
    await upload_file(storage, bucket, tmp_file)

    await clean_results(
        measurement_uuid=measurement_uuid, agent_uuid=agent_uuid, storage=storage
    )
    assert not await storage.bucket_exists(bucket)
