from uuid import uuid4

from iris.commons.models import Target
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
from tests.assertions import assert_response, assert_status_code, cast_response
from tests.helpers import (
    add_and_refresh,
    archive_target_file,
    create_user_buckets,
    register_agent,
    upload_target_file,
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
        make_measurement(user_id=str(uuid4())),
    ]
    add_and_refresh(session, measurements)

    # We expect only the measurements of our user
    measurements = sorted(
        measurements[:-1], key=lambda x: x.creation_time, reverse=True
    )
    expected = Paginated[MeasurementRead](
        count=len(measurements),
        results=MeasurementRead.from_measurements(measurements),
    )
    assert_response(client.get("/measurements"), expected)


def test_get_measurements_with_state(
    make_client, make_measurement, make_measurement_agent, make_user, session
):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurements = [
        make_measurement(
            user_id=str(user.id),
            agents=[make_measurement_agent(state=MeasurementAgentState.Created)],
        ),
        make_measurement(
            user_id=str(user.id),
            agents=[make_measurement_agent(state=MeasurementAgentState.Finished)],
        ),
    ]
    add_and_refresh(session, measurements)

    expected = Paginated[MeasurementRead](
        count=1,
        results=MeasurementRead.from_measurements(measurements[1:2]),
    )
    assert_response(
        client.get("/measurements", params={"state": "finished"}),
        expected,
    )


def test_get_measurements_with_tag(make_client, make_measurement, make_user, session):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurements = [
        make_measurement(user_id=str(user.id)),
        make_measurement(user_id=str(user.id)),
        make_measurement(user_id=str(user.id), tags=["mytag"]),
    ]
    add_and_refresh(session, measurements)

    expected = Paginated[MeasurementRead](
        count=1,
        results=MeasurementRead.from_measurements(measurements[2:3]),
    )
    assert_response(client.get("/measurements", params={"tag": "mytag"}), expected)


def test_get_measurements_public(make_client, make_measurement, make_user, session):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurements = [
        make_measurement(user_id=str(user.id)),
        make_measurement(user_id=str(user.id)),
        make_measurement(user_id=str(user.id), tags=["visibility:public"]),
        make_measurement(user_id=str(uuid4()), tags=["visibility:public"]),
    ]
    add_and_refresh(session, measurements)

    # We expect only the public measurements
    measurements = sorted(measurements[2:], key=lambda x: x.creation_time, reverse=True)
    expected = Paginated[MeasurementRead](
        count=len(measurements),
        results=MeasurementRead.from_measurements(measurements),
    )
    assert_response(client.get("/measurements/public"), expected)


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


async def test_get_measurement_verified_user(
    make_client, make_measurement, make_user, redis, session, storage
):
    user = make_user(probing_enabled=False)
    client = make_client(user)

    measurement = make_measurement(user_id=str(user.id))
    add_and_refresh(session, [measurement])

    assert_response(
        client.get(f"/measurements/{measurement.uuid}"),
        MeasurementReadWithAgents.from_measurement(measurement),
    )


async def test_get_measurement_other_user(
    make_client, make_measurement, make_user, redis, session, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    measurement = make_measurement(user_id=str(uuid4()))
    add_and_refresh(session, [measurement])
    assert_status_code(client.get(f"/measurements/{measurement.uuid}"), 404)


async def test_get_measurement_not_found(
    make_client, make_measurement, make_user, redis, session, storage
):
    client = make_client(make_user(probing_enabled=True))
    assert_status_code(client.get(f"/measurements/{uuid4()}"), 404)


async def test_get_measurement_agent_target(
    make_client, make_measurement, make_user, redis, session, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurement = make_measurement(user_id=str(user.id))
    measurement_agent = measurement.agents[0]
    add_and_refresh(session, [measurement])

    await create_user_buckets(storage, user)
    await upload_target_file(
        storage, user, measurement_agent.target_file, ["0.0.0.0/0,icmp,8,32,6"]
    )
    await archive_target_file(
        storage,
        user,
        measurement_agent.measurement_uuid,
        measurement_agent.agent_uuid,
        measurement_agent.target_file,
    )

    response = client.get(
        f"/measurements/{measurement_agent.measurement_uuid}/{measurement_agent.agent_uuid}/target"
    )
    target = cast_response(response, Target)
    assert target.content == ["0.0.0.0/0,icmp,8,32,6"]


async def test_cancel_measurement(
    make_client, make_measurement, make_user, session, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurement = make_measurement(user_id=str(user.id))
    add_and_refresh(session, [measurement])

    expected = MeasurementReadWithAgents.from_measurement(measurement)
    expected.state = MeasurementAgentState.Canceled
    for agent in expected.agents:
        agent.state = MeasurementAgentState.Canceled

    # DELETE should be idempotent
    assert_status_code(client.delete(f"/measurements/{measurement.uuid}"), 200)
    assert_status_code(client.delete(f"/measurements/{measurement.uuid}"), 200)

    actual = cast_response(
        client.delete(f"/measurements/{measurement.uuid}"), MeasurementReadWithAgents
    )
    assert actual.creation_time
    assert not actual.start_time
    assert actual.end_time


async def test_cancel_measurement_not_found(
    make_client, make_measurement, make_user, session, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    assert_status_code(client.delete(f"/measurements/{uuid4()}"), 404)


async def test_patch_measurement(
    make_client, make_measurement, make_user, redis, session, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurement = make_measurement(user_id=str(user.id))
    add_and_refresh(session, [measurement])

    response_before_patch = client.get(f"/measurements/{measurement.uuid}").json()
    assert measurement.tags == response_before_patch["tags"]

    response_after_patch = client.patch(
        f"/measurements/{measurement.uuid}", json={"tags": ["test"]}
    ).json()
    assert response_after_patch["tags"] == ["test"]


async def test_patch_measurement_public_tag_disallowed(
    make_client, make_measurement, make_user, redis, session, storage
):
    user = make_user(allow_tag_public=False, probing_enabled=True)
    client = make_client(user)

    measurement = make_measurement(user_id=str(user.id))
    add_and_refresh(session, [measurement])

    response = client.patch(
        f"/measurements/{measurement.uuid}", json={"tags": ["visibility:public"]}
    )
    assert_status_code(response, 403)
    assert "You cannot use public tag" in response.text


async def test_patch_measurement_reserved_tag_disallowed(
    make_client, make_measurement, make_user, redis, session, storage
):
    user = make_user(allow_tag_reserved=False, probing_enabled=True)
    client = make_client(user)

    measurement = make_measurement(user_id=str(user.id))
    add_and_refresh(session, [measurement])

    response = client.patch(
        f"/measurements/{measurement.uuid}", json={"tags": ["collection:test"]}
    )
    assert_status_code(response, 403)
    assert "You cannot use reserved tags" in response.text


async def test_patch_measurement_no_tag_in_body(
    make_client, make_measurement, make_user, redis, session, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)

    measurement = make_measurement(user_id=str(user.id))
    add_and_refresh(session, [measurement])

    response_before_patch = client.get(f"/measurements/{measurement.uuid}").json()
    assert measurement.tags == response_before_patch["tags"]

    response_after_patch = client.patch(
        f"/measurements/{measurement.uuid}", json={"toto": ["test"]}
    ).json()

    assert measurement.tags == response_after_patch["tags"]


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


async def test_post_measurement_public_tag_disallowed(make_client, make_user):
    client = make_client(make_user(allow_tag_public=False, probing_enabled=True))
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        tags=["visibility:public"],
        agents=[MeasurementAgentCreate(uuid=str(uuid4()), target_file="targets.csv")],
    )
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 403)
    assert "You cannot use public tag" in response.text


async def test_post_measurement_reserved_tag_disallowed(make_client, make_user):
    client = make_client(make_user(allow_tag_reserved=False, probing_enabled=True))
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        tags=["collection:test"],
        agents=[MeasurementAgentCreate(uuid=str(uuid4()), target_file="targets.csv")],
    )
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 403)
    assert "You cannot use reserved tags" in response.text


# TODO: test_post_measurement_unknown_target_file


async def test_post_measurement_uuid(
    make_client, make_user, make_agent_parameters, redis, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    agent_uuid = str(uuid4())
    await register_agent(redis, agent_uuid, make_agent_parameters(), AgentState.Idle)
    await create_user_buckets(storage, user)
    await upload_target_file(storage, user, "targets.csv")
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentCreate(uuid=agent_uuid, target_file="targets.csv")],
    )
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 201)
    result = cast_response(response, MeasurementReadWithAgents)
    assert result.state == MeasurementAgentState.Created
    assert not result.start_time
    assert not result.end_time


async def test_post_measurement_tag(
    make_client, make_user, make_agent_parameters, redis, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    # We check with multiple agents since we encountered a bug where tag selection
    # was seemingly working, but was in practice selecting only one matching agent.
    await register_agent(
        redis, str(uuid4()), make_agent_parameters(tags=["tag1"]), AgentState.Idle
    )
    await register_agent(
        redis, str(uuid4()), make_agent_parameters(tags=["tag1"]), AgentState.Idle
    )
    await create_user_buckets(storage, user)
    await upload_target_file(storage, user, "targets.csv")
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentCreate(tag="tag1", target_file="targets.csv")],
    )
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 201)
    result = cast_response(response, MeasurementReadWithAgents)
    assert result.state == MeasurementAgentState.Created
    assert len(result.agents) == 2
    assert not result.start_time
    assert not result.end_time


async def test_post_measurement_duplicate(
    make_client, make_user, make_agent_parameters, redis, storage
):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    agent_uuid = str(uuid4())
    await register_agent(
        redis, agent_uuid, make_agent_parameters(tags=["tag1"]), AgentState.Idle
    )
    await create_user_buckets(storage, user)
    await upload_target_file(storage, user, "targets.csv")
    body = MeasurementCreate(
        tool=Tool.DiamondMiner,
        agents=[
            MeasurementAgentCreate(tag="tag1", target_file="targets.csv"),
            MeasurementAgentCreate(uuid=agent_uuid, target_file="targets.csv"),
        ],
    )
    response = client.post("/measurements/", data=body.json())
    assert_status_code(response, 400)
    assert "Multiple assignment of key" in response.text
