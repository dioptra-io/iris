from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List
from uuid import uuid4

import pytest

from iris.commons.models.agent import AgentState
from iris.commons.models.diamond_miner import Tool
from iris.commons.models.measurement import (
    Measurement,
    MeasurementCreate,
    MeasurementRead,
    MeasurementReadWithAgents,
)
from iris.commons.models.measurement_agent import (
    MeasurementAgentCreate,
    MeasurementAgentState,
)
from iris.commons.models.pagination import Paginated
from iris.commons.models.user import User
from iris.commons.storage import Storage, targets_key
from tests.assertions import assert_response, assert_status_code
from tests.helpers import add_and_refresh

pytestmark = pytest.mark.asyncio

# TODO: Mock send


# TODO: Move to helpers?
async def upload_target_file(
    storage: Storage, user: User, filename: str, content: List[str]
):
    await storage.create_bucket(storage.targets_bucket(str(user.id)))
    with TemporaryDirectory() as directory:
        file = Path(directory) / filename
        file.write_text("\n".join(content))
        await storage.upload_file(
            storage.targets_bucket(str(user.id)), filename, str(file)
        )


async def archive_target_file(
    storage: Storage, user: User, measurement_uuid: str, agent_uuid: str, filename: str
):
    await storage.create_bucket(storage.archive_bucket(str(user.id)))
    await storage.copy_file_to_bucket(
        storage.targets_bucket(str(user.id)),
        storage.archive_bucket(str(user.id)),
        filename,
        targets_key(measurement_uuid, agent_uuid),
    )


def test_get_measurements_probing_not_enabled(make_client, make_user):
    client = make_client(make_user(probing_enabled=False))
    assert_status_code(client.get("/measurements"), 403)


def test_get_measurements_empty(make_client, make_user):
    client = make_client(make_user(probing_enabled=True))
    assert_response(
        client.get("/measurements"), Paginated[Measurement](count=0, results=[])
    )


def test_get_measurements(make_client, make_measurement, make_user, session):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurements = [
        make_measurement(user_id=str(user.id)),
        make_measurement(user_id=str(user.id)),
        make_measurement(user_id=str(user.id)),
    ]
    add_and_refresh(session, measurements)

    expected = Paginated[MeasurementRead](
        count=len(measurements),
        results=MeasurementRead.from_measurements(measurements),
    )
    assert_response(client.get("/measurements"), expected)


async def test_get_measurement(
    make_client, make_measurement, make_user, redis, session, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurement = make_measurement(user_id=str(user.id))
    add_and_refresh(session, [measurement])

    assert_response(
        client.get(f"/measurements/{measurement.uuid}"),
        MeasurementReadWithAgents.from_measurement(measurement),
    )


# TODO: test get_measurement_agent_target
# agents_read = []
# for agent in measurement.agents:
#     if state != MeasurementAgentState.Unknown:
#         agent.state = state
#     await upload_target_file(storage, user, agent.target_file, ["0.0.0.0"])
#     await archive_target_file(
#         storage,
#         user,
#         measurement.uuid,
#         agent.agent_uuid,
#         agent.target_file,
#     )
#     agents_read.append(
#         await MeasurementAgentRead.from_measurement_agent(
#             agent, storage, str(user.id)
#         )
#     )

# TODO: Test measurement_not_found and measurement_other_user


async def test_delete_measurement(
    make_client, make_measurement, make_user, session, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurement = make_measurement(user_id=str(user.id))
    add_and_refresh(session, [measurement])

    # DELETE should be idempotent
    assert_status_code(client.delete(f"/measurements/{measurement.uuid}"), 204)
    assert_status_code(client.delete(f"/measurements/{measurement.uuid}"), 204)

    expected = MeasurementReadWithAgents.from_measurement(measurement)
    expected.state = MeasurementAgentState.Canceled
    assert_response(client.get(f"/measurements/{measurement.uuid}"), expected)


async def test_post_measurement_unknown_uuid(make_client, make_user):
    client = make_client(make_user(probing_enabled=True))
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        tags=[],
        agents=[MeasurementAgentCreate(uuid=str(uuid4()), target_file="targets.csv")],
    )
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 404)
    assert "No agent associated with UUID" in response.text


async def test_post_measurement_unknown_tag(make_client, make_user):
    client = make_client(make_user(probing_enabled=True))
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        tags=[],
        agents=[MeasurementAgentCreate(tag="unknown", target_file="targets.csv")],
    )
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 404)
    assert "No agents associated with tag" in response.text


# TODO: test_post_measurement_unknown_target_file


async def test_post_measurement_uuid(
    make_client, make_user, make_agent_parameters, redis, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    agent_uuid = str(uuid4())
    await redis.register_agent(agent_uuid, 5)
    await redis.set_agent_parameters(agent_uuid, make_agent_parameters())
    await redis.set_agent_state(agent_uuid, AgentState.Idle)
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentCreate(uuid=agent_uuid, target_file="targets.csv")],
    )
    await upload_target_file(storage, user, "targets.csv", ["0.0.0.0/0,icmp,8,32,6"])
    # TODO: Best place to create the test buckets?
    await storage.create_bucket(storage.archive_bucket(str(user.id)))
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 201)
    # TODO: Assert response object


async def test_post_measurement_tag(
    make_client, make_user, make_agent_parameters, redis, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    agent_uuid = str(uuid4())
    await redis.register_agent(agent_uuid, 5)
    await redis.set_agent_parameters(agent_uuid, make_agent_parameters(tags=["tag1"]))
    await redis.set_agent_state(agent_uuid, AgentState.Idle)
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentCreate(tag="tag1", target_file="targets.csv")],
    )
    await upload_target_file(storage, user, "targets.csv", ["0.0.0.0/0,icmp,8,32,6"])
    # TODO: Best place to create the test buckets?
    await storage.create_bucket(storage.archive_bucket(str(user.id)))
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 201)
    # TODO: Assert response object


async def test_post_measurement_duplicate(
    make_client, make_user, make_agent_parameters, redis, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    agent_uuid = str(uuid4())
    await redis.register_agent(agent_uuid, 5)
    await redis.set_agent_parameters(agent_uuid, make_agent_parameters(tags=["tag1"]))
    await redis.set_agent_state(agent_uuid, AgentState.Idle)
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        agents=[
            MeasurementAgentCreate(tag="tag1", target_file="targets.csv"),
            MeasurementAgentCreate(uuid=agent_uuid, target_file="targets.csv"),
        ],
    )
    await upload_target_file(storage, user, "targets.csv", ["0.0.0.0/0,icmp,8,32,6"])
    # TODO: Best place to create the test buckets?
    await storage.create_bucket(storage.archive_bucket(str(user.id)))
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 400)
    assert "Multiple assignment of key" in response.text
