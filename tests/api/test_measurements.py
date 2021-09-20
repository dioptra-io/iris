import uuid
from datetime import datetime

import pytest

from iris.api.dependencies import get_redis, get_storage
from iris.api.measurements import verify_quota
from iris.api.security import get_current_active_user
from iris.commons.database import Replies, agents, measurements
from iris.commons.schemas.public import (
    FlowMapper,
    Measurement,
    MeasurementAgent,
    MeasurementAgentPostBody,
    MeasurementAgentSpecific,
    MeasurementPostBody,
    MeasurementState,
    MeasurementSummary,
    Paginated,
    Reply,
    Tool,
    ToolParameters,
)
from tests.helpers import async_mock, fake_redis_factory, fake_storage_factory, override


class FakeSend(object):
    def send(*args, **kwargs):
        pass


target23 = {
    "key": "test.csv",
    "size": 42,
    "content": "8.8.8.0/23,icmp,2,32",
    "last_modified": "Mon, 20 Sep 2021 13:20:26 GMT",
    "metadata": None,
}

target25 = {
    "key": "test.csv",
    "size": 42,
    "content": "8.8.8.0/25,icmp,2,32",
    "last_modified": "Mon, 20 Sep 2021 13:20:26 GMT",
    "metadata": None,
}

target_probes = {
    "key": "probes.csv",
    "size": 42,
    "content": "8.8.8.8,24000,33434,32,icmp",
    "last_modified": "Mon, 20 Sep 2021 13:20:26 GMT",
    "metadata": {"is_probes_file": "True"},
}


@pytest.fixture(scope="function")
def measurement1():
    return Measurement(
        uuid=uuid.uuid4(),
        username="test",
        state=MeasurementState.Unknown,
        tool=Tool.DiamondMiner,
        agents=[],
        tags=["test"],
        start_time=datetime.now(),
        end_time=datetime.now(),
    )


@pytest.fixture(scope="function")
def measurement_agent1(agent, statistics):
    return MeasurementAgent(
        uuid=agent.uuid,
        state=MeasurementState.Unknown,
        specific=MeasurementAgentSpecific(
            target_file="test.csv",
            target_file_content=["8.8.8.0/23,icmp,2,32"],
            probing_rate=None,
            tool_parameters=ToolParameters(
                initial_source_port=24000,
                destination_port=33434,
                flow_mapper=FlowMapper.IntervalFlowMapper,
                flow_mapper_kwargs={},
                max_round=5,
                n_flow_ids=6,
                global_min_ttl=0,
                global_max_ttl=255,
            ),
        ),
        parameters=agent.parameters,
        probing_statistics=[statistics],
    )


# --- GET /api/measurements ---


def test_get_measurements_empty(api_client_sync, monkeypatch):
    monkeypatch.setattr(measurements, "all", async_mock([]))
    monkeypatch.setattr(measurements, "all_count", async_mock(0))
    response = api_client_sync.get("/api/measurements")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=0, results=[]
    )


def test_get_measurements(api_client_sync, monkeypatch):
    measurements_ = [
        Measurement(
            uuid=uuid.uuid4(),
            username="test",
            state=MeasurementState.Finished,
            tool=Tool.DiamondMiner,
            agents=[],
            tags=[],
            start_time=datetime.now(),
            end_time=datetime.now(),
        ),
        Measurement(
            uuid=uuid.uuid4(),
            username="test",
            state=MeasurementState.Finished,
            tool=Tool.DiamondMiner,
            agents=[],
            tags=[],
            start_time=datetime.now(),
            end_time=datetime.now(),
        ),
        Measurement(
            uuid=uuid.uuid4(),
            username="test",
            state=MeasurementState.Finished,
            tool=Tool.DiamondMiner,
            agents=[],
            tags=[],
            start_time=datetime.now(),
            end_time=datetime.now(),
        ),
    ]

    measurements_ = sorted(measurements_, key=lambda x: x.start_time, reverse=True)
    summaries = [
        MeasurementSummary(**x.dict(exclude={"agents", "username"}))
        for x in measurements_
    ]

    async def all(self, user, offset, limit, tag=None):
        return measurements_[offset : offset + limit]  # noqa : E203

    override(
        api_client_sync,
        get_redis,
        fake_redis_factory(measurement_state=MeasurementState.Finished),
    )
    monkeypatch.setattr(measurements, "all", all)
    monkeypatch.setattr(measurements, "all_count", async_mock(3))

    # No (offset, limit)
    response = api_client_sync.get("/api/measurements")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3, results=summaries
    )

    # All inclusive (0, 100)
    response = api_client_sync.get("/api/measurements?offset=0&limit=100")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3, results=summaries
    )

    # First result (0, 1)
    response = api_client_sync.get("/api/measurements?offset=0&limit=1")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3,
        next="http://testserver/api/measurements/?limit=1&offset=1",
        results=summaries[:1],
    )

    # Middle result (1, 1)
    response = api_client_sync.get("/api/measurements?offset=1&limit=1")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3,
        next="http://testserver/api/measurements/?limit=1&offset=2",
        previous="http://testserver/api/measurements/?limit=1",
        results=summaries[1:2],
    )

    # Last result (2, 1)
    response = api_client_sync.get("/api/measurements?offset=2&limit=1")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3,
        previous="http://testserver/api/measurements/?limit=1&offset=1",
        results=summaries[2:3],
    )


# --- POST /api/measurements/ ---


@pytest.mark.asyncio
async def test_verify_quota():
    assert await verify_quota("8.8.8.0/23,icmp,2,32", 24, 64, 2) is True
    assert await verify_quota("8.8.8.0/23,icmp,2,32", 24, 64, 1) is False
    assert await verify_quota("8.8.8.0/24", 32, 128, 256) is True
    assert await verify_quota("8.8.8.0/24", 32, 128, 255) is False


def test_post_measurement(api_client_sync, agent, monkeypatch):
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())
    for tool in Tool:
        if tool == Tool.Probes:
            continue
        body = MeasurementPostBody(
            tool=Tool(tool),
            agents=[
                MeasurementAgentPostBody(
                    uuid=agent.uuid,
                    target_file="test.csv",
                    tool_parameters=ToolParameters(
                        n_flow_ids=6 if tool == Tool.DiamondMiner else 1,
                        prefix_len_v4=32 if tool == Tool.Ping else 24,
                        prefix_len_v6=128 if tool == Tool.Ping else 64,
                    ),
                )
            ],
        )
        response = api_client_sync.post("/api/measurements/", data=body.json())
        assert response.status_code == 201


def test_post_measurement_probes(api_client_sync, agent, monkeypatch):
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target_probes]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    body = MeasurementPostBody(
        tool=Tool.Probes,
        agents=[
            MeasurementAgentPostBody(
                uuid=agent.uuid,
                target_file="test.csv",
                tool_parameters=ToolParameters(
                    n_flow_ids=1,
                    prefix_len_v4=32,
                    prefix_len_v6=128,
                ),
            )
        ],
    )
    response = api_client_sync.post("/api/measurements/", data=body.json())
    assert response.status_code == 201


def test_post_measurement_quota_exceeded(api_client_sync, agent, user, monkeypatch):
    user = user.copy(update={"is_admin": False, "quota": 0})
    override(api_client_sync, get_current_active_user, lambda: user)
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())
    body = MeasurementPostBody(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentPostBody(uuid=agent.uuid, target_file="test.csv")],
    )
    response = api_client_sync.post("/api/measurements/", data=body.json())
    assert response.status_code == 403


def test_post_measurement_diamond_miner_invalid_prefix_length(
    api_client_sync, agent, monkeypatch
):
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target25]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())
    body = MeasurementPostBody(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentPostBody(uuid=agent.uuid, target_file="test.csv")],
    )
    response = api_client_sync.post("/api/measurements/", data=body.json())
    assert response.status_code == 403


def test_post_measurement_agent_tag(api_client_sync, agent, monkeypatch):
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())
    body = MeasurementPostBody(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentPostBody(tag="test", target_file="test.csv")],
    )
    response = api_client_sync.post("/api/measurements/", data=body.json())
    assert response.status_code == 201


def test_post_measurement_with_agent_not_found(api_client_sync, monkeypatch):
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())
    body = MeasurementPostBody(
        tool=Tool.DiamondMiner,
        agents=[
            MeasurementAgentPostBody(
                uuid=uuid.UUID("6f4ed428-8de6-460e-9e19-6e6173776550"),
                target_file="test.csv",
            )
        ],
    )
    response = api_client_sync.post("/api/measurements/", data=body.json())
    assert response.status_code == 404
    assert response.json() == {
        "detail": "No agent associated with UUID 6f4ed428-8de6-460e-9e19-6e6173776550"
    }


def test_post_measurement_tag_not_found(api_client_sync, monkeypatch):
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())
    body = MeasurementPostBody(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentPostBody(tag="toto", target_file="test.csv")],
    )
    response = api_client_sync.post("/api/measurements/", data=body.json())
    assert response.status_code == 404
    assert response.json() == {"detail": "No agent associated with tag toto"}


def test_post_measurement_agent_multiple_definition(
    api_client_sync, agent, monkeypatch
):
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())
    body = MeasurementPostBody(
        tool=Tool.DiamondMiner,
        agents=[
            MeasurementAgentPostBody(tag="test", target_file="test.csv"),
            MeasurementAgentPostBody(uuid=agent.uuid, target_file="test.csv"),
        ],
    )
    response = api_client_sync.post("/api/measurements/", data=body.json())
    assert response.status_code == 400
    assert response.json() == {"detail": f"Multiple definition of agent `{agent.uuid}`"}


def test_post_measurement_target_file_not_found(api_client_sync, agent, monkeypatch):
    override(api_client_sync, get_storage, fake_storage_factory([]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())
    body = MeasurementPostBody(
        tool=Tool.DiamondMiner,
        agents=[MeasurementAgentPostBody(uuid=agent.uuid, target_file="test.csv")],
    )
    response = api_client_sync.post("/api/measurements/", data=body.json())
    assert response.status_code == 404


# --- GET /api/measurements/{measurement_uuid} ---


def test_get_measurement_by_uuid(
    api_client_sync, measurement1, measurement_agent1, monkeypatch
):
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr(agents, "all", async_mock([measurement_agent1]))
    monkeypatch.setattr(measurements, "get", async_mock(measurement1))
    expected = measurement1.copy(update={"agents": [measurement_agent1]})
    response = api_client_sync.get(f"/api/measurements/{measurement1.uuid}")
    assert Measurement(**response.json()) == expected


def test_get_measurement_by_uuid_waiting(
    api_client_sync, measurement1, measurement_agent1, monkeypatch
):
    override(
        api_client_sync,
        get_redis,
        fake_redis_factory(measurement_state=MeasurementState.Waiting),
    )
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr(agents, "all", async_mock([measurement_agent1]))
    monkeypatch.setattr(measurements, "get", async_mock(measurement1))
    expected = measurement1.copy(
        update={
            "state": MeasurementState.Waiting,
            "agents": [
                measurement_agent1.copy(update={"state": MeasurementState.Waiting})
            ],
        }
    )
    response = api_client_sync.get(f"/api/measurements/{measurement1.uuid}")
    assert Measurement(**response.json()).dict() == expected.dict()


def test_get_measurement_by_uuid_not_found(api_client_sync, monkeypatch):
    monkeypatch.setattr(measurements, "get", async_mock(None))
    measurement_uuid = str(uuid.uuid4())
    response = api_client_sync.get(f"/api/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_get_measurement_by_uuid_invalid_input(api_client_sync):
    response = api_client_sync.get("/api/measurements/test")
    assert response.status_code == 422


# -- DELETE /api/measurements/{measurement_uuid}/{agent_uuid} ---


def test_delete_measurement_by_uuid(api_client_sync, monkeypatch):
    override(
        api_client_sync,
        get_redis,
        fake_redis_factory(measurement_state=MeasurementState.Ongoing),
    )
    monkeypatch.setattr(measurements, "get", async_mock({"uuid": "uuid"}))
    measurement_uuid = str(uuid.uuid4())
    response = api_client_sync.delete(f"/api/measurements/{measurement_uuid}")
    assert response.json() == {"uuid": measurement_uuid, "action": "canceled"}


def test_delete_measurement_by_uuid_not_found(api_client_sync, monkeypatch):
    monkeypatch.setattr(measurements, "get", async_mock(None))
    measurement_uuid = str(uuid.uuid4())
    response = api_client_sync.delete(f"/api/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_delete_measurement_by_uuid_already_finished(api_client_sync, monkeypatch):
    override(api_client_sync, get_redis, fake_redis_factory())
    monkeypatch.setattr(measurements, "get", async_mock({"uuid": "uuid"}))
    measurement_uuid = str(uuid.uuid4())
    response = api_client_sync.delete(f"/api/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement already finished"}


# --- GET /api/measurements/{measurement_uuid}/{agent_uuid} ---


def test_get_measurement_results(
    api_client_sync, measurement1, measurement_agent1, monkeypatch
):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())
    results = [
        {
            "probe_protocol": "icmp",
            "probe_src_addr": "::ffff:ac12:b",
            "probe_dst_addr": "::ffff:84e3:7b81",
            "probe_src_port": 24000,
            "probe_dst_port": 34334,
            "probe_ttl": 78,
            "quoted_ttl": 9,
            "reply_src_addr": "::ffff:869d:fe0a",
            "reply_protocol": "udp",
            "reply_icmp_type": 11,
            "reply_icmp_code": 0,
            "reply_ttl": 37,
            "reply_size": 56,
            "reply_mpls_labels": [1],
            "rtt": 1280.2,
            "round": 1,
        }
    ]

    monkeypatch.setattr(measurements, "get", async_mock(measurement1))
    monkeypatch.setattr(
        agents,
        "get",
        async_mock(
            measurement_agent1.copy(update={"state": MeasurementState.Finished})
        ),
    )
    monkeypatch.setattr(Replies, "exists", async_mock(True))
    monkeypatch.setattr(Replies, "all", async_mock(results))
    monkeypatch.setattr(Replies, "all_count", async_mock(1))

    response = api_client_sync.get(
        f"/api/results/{measurement_uuid}/{agent_uuid}/replies/0.0.0.0"
    )
    assert Paginated[Reply](**response.json()) == Paginated(count=1, results=results)


def test_get_measurement_results_table_not_exists(
    api_client_sync, measurement1, measurement_agent1, monkeypatch
):
    monkeypatch.setattr(measurements, "get", async_mock(measurement1))
    monkeypatch.setattr(
        agents,
        "get",
        async_mock(
            measurement_agent1.copy(update={"state": MeasurementState.Finished})
        ),
    )
    monkeypatch.setattr(Replies, "exists", async_mock(0))
    response = api_client_sync.get(
        f"/api/results/{measurement1.uuid}/{measurement_agent1.uuid}/replies/0.0.0.0"
    )
    assert Paginated[Reply](**response.json()) == Paginated(count=0, results=[])


def test_get_measurement_results_not_finished(
    api_client_sync, measurement1, measurement_agent1, monkeypatch
):
    monkeypatch.setattr(measurements, "get", async_mock(measurement1))
    monkeypatch.setattr(
        agents,
        "get",
        async_mock(measurement_agent1.copy(update={"state": MeasurementState.Ongoing})),
    )
    response = api_client_sync.get(
        f"/api/results/{measurement1.uuid}/{measurement_agent1.uuid}/replies/0.0.0.0"
    )
    assert response.status_code == 412


def test_get_measurement_results_no_agent(
    api_client_sync, measurement1, measurement_agent1, monkeypatch
):
    monkeypatch.setattr(measurements, "get", async_mock(measurement1))
    monkeypatch.setattr(agents, "get", async_mock(None))

    response = api_client_sync.get(
        f"/api/results/{measurement1.uuid}/{measurement_agent1.uuid}/replies/0.0.0.0"
    )
    assert response.status_code == 404
    assert response.json() == {
        "detail": (
            f"The agent `{measurement_agent1.uuid}` "
            f"did not participate to measurement `{measurement1.uuid}`"
        )
    }


def test_get_measurement_result_not_found(api_client_sync, monkeypatch):
    monkeypatch.setattr(measurements, "get", async_mock(None))
    response = api_client_sync.get(
        f"/api/results/{uuid.uuid4()}/{uuid.uuid4()}/replies/0.0.0.0"
    )
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_get_measurement_results_invalid_measurement_uuid(api_client_sync):
    response = api_client_sync.get(
        f"/api/results/invalid_uuid/{uuid.uuid4()}/replies/0.0.0.0"
    )
    assert response.status_code == 422


def test_get_measurement_results_invalid_agent_uuid(api_client_sync):
    response = api_client_sync.get(
        f"/api/results/{uuid.uuid4()}/invalid_uuid/replies/0.0.0.0"
    )
    assert response.status_code == 422
