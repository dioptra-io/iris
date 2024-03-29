from uuid import uuid4

from iris.commons.models.agent import AgentState
from iris.commons.models.round import Round
from iris.commons.storage import results_key
from iris.worker.watch import check_agent, find_results, watch_measurement_agent_
from tests.helpers import register_agent, upload_file


async def test_check_agent_offline(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    assert not await check_agent(
        redis=redis, agent_uuid=agent_uuid, trials=3, interval=0.1
    )


async def test_check_agent_online(redis, make_agent_parameters):
    agent_uuid = str(uuid4())
    await register_agent(redis, agent_uuid, make_agent_parameters(), AgentState.Working)
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


async def test_watch_measurement_not_found(caplog, engine, worker_settings):
    await watch_measurement_agent_(str(uuid4()), str(uuid4()), worker_settings)
    assert "Measurement not found" in caplog.text
