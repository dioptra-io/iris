import uuid
from datetime import datetime

import pytest

from iris.api.dependencies import get_redis, get_storage
from iris.api.measurements import verify_quota
from iris.api.security import get_current_active_user
from iris.commons.database import Agents, Measurements, Replies
from iris.commons.schemas.public import MeasurementState, Tool
from tests.helpers import async_mock, fake_redis_factory, fake_storage_factory, override


class FakeSend(object):
    def send(*args, **kwargs):
        pass


target23 = {
    "key": "test.csv",
    "size": 42,
    "content": "8.8.8.0/23,icmp,2,32",
    "last_modified": "test",
}

target25 = {
    "key": "test.csv",
    "size": 42,
    "content": "8.8.8.0/25,icmp,2,32",
    "last_modified": "test",
}

# --- GET /api/measurements ---


def test_get_measurements_empty(api_client_sync, monkeypatch):
    monkeypatch.setattr(Measurements, "all", async_mock([]))
    monkeypatch.setattr(Measurements, "all_count", async_mock(0))
    response = api_client_sync.get("/api/measurements")
    assert response.json() == {
        "count": 0,
        "next": None,
        "previous": None,
        "results": [],
    }


def test_get_measurements(api_client_sync, monkeypatch):
    measurements = [
        {
            "uuid": str(uuid.uuid4()),
            "state": "finished",
            "tool": "diamond-miner",
            "tags": [],
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        },
        {
            "uuid": str(uuid.uuid4()),
            "state": "finished",
            "tool": "diamond-miner",
            "tags": [],
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        },
        {
            "uuid": str(uuid.uuid4()),
            "state": "finished",
            "tool": "diamond-miner",
            "tags": ["test"],
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        },
    ]

    measurements = sorted(measurements, key=lambda x: x["start_time"], reverse=True)

    async def all(self, user, offset, limit, tag=None):
        return measurements[offset : offset + limit]  # noqa : E203

    override(
        api_client_sync,
        get_redis,
        fake_redis_factory(measurement_state=MeasurementState.Finished),
    )
    monkeypatch.setattr(Measurements, "all", all)
    monkeypatch.setattr(Measurements, "all_count", async_mock(3))

    # No (offset, limit)
    response = api_client_sync.get("/api/measurements")
    assert response.json() == {
        "count": 3,
        "next": None,
        "previous": None,
        "results": measurements,
    }

    # All inclusive (0, 100)
    response = api_client_sync.get("/api/measurements?offset=0&limit=100")
    assert response.json() == {
        "count": 3,
        "next": None,
        "previous": None,
        "results": measurements,
    }

    # First result (0, 1)
    response = api_client_sync.get("/api/measurements?offset=0&limit=1")
    assert response.json() == {
        "count": 3,
        "next": "http://testserver/api/measurements/?limit=1&offset=1",
        "previous": None,
        "results": measurements[0:1],
    }

    # Middle result (1, 1)
    response = api_client_sync.get("/api/measurements?offset=1&limit=1")
    assert response.json() == {
        "count": 3,
        "next": "http://testserver/api/measurements/?limit=1&offset=2",
        "previous": "http://testserver/api/measurements/?limit=1",
        "results": measurements[1:2],
    }

    # Last result (2, 1)
    response = api_client_sync.get("/api/measurements?offset=2&limit=1")
    assert response.json() == {
        "count": 3,
        "next": None,
        "previous": "http://testserver/api/measurements/?limit=1&offset=1",
        "results": measurements[2:3],
    }


# --- POST /api/measurements/ ---


@pytest.mark.asyncio
async def test_verify_quota():
    assert await verify_quota("diamond-miner", "8.8.8.0/23,icmp,2,32", 2) is True
    assert await verify_quota("diamond-miner", "8.8.8.0/23,icmp,2,32", 1) is False
    assert await verify_quota("ping", "8.8.8.0/24", 256) is True
    assert await verify_quota("ping", "8.8.8.0/24", 255) is False


def test_post_measurement(api_client_sync, agent, monkeypatch):
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    for tool in Tool:
        response = api_client_sync.post(
            "/api/measurements/",
            json={
                "tool": tool,
                "agents": [
                    {
                        "uuid": str(agent.uuid),
                        "target_file": "test.csv",
                    }
                ],
            },
        )
        assert response.status_code == 201


def test_post_measurement_quota_exceeded(api_client_sync, agent, user, monkeypatch):
    user = user.copy(update={"is_admin": False, "quota": 0})
    override(api_client_sync, get_current_active_user, lambda: user)
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    for tool in Tool:
        response = api_client_sync.post(
            "/api/measurements/",
            json={
                "tool": tool,
                "agents": [
                    {
                        "uuid": str(agent.uuid),
                        "target_file": "test.csv",
                    }
                ],
            },
        )
        assert response.status_code == 403


def test_post_measurement_diamond_miner_invalid_prefix_length(
    api_client_sync, agent, monkeypatch
):
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target25]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = api_client_sync.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": str(agent.uuid),
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 403


def test_post_measurement_agent_tag(api_client_sync, agent, monkeypatch):
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = api_client_sync.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "tag": "test",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 201


def test_post_measurement_no_uuid_or_tag(api_client_sync, monkeypatch):
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = api_client_sync.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 422
    assert (
        response.json()["detail"][0]["msg"]
        == "one of `uuid` or `tag` must be specified"
    )


def test_post_measurement_tag_and_uuid(api_client_sync, monkeypatch):
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = api_client_sync.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776550",
                    "tag": "test",
                    "target_file": "test.csv",
                }
            ],
        },
    )

    assert response.status_code == 422
    assert (
        response.json()["detail"][0]["msg"]
        == "one of `uuid` or `tag` must be specified"
    )


def test_post_measurement_with_agent_not_found(api_client_sync, monkeypatch):
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = api_client_sync.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776550",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 404
    assert response.json() == {
        "detail": "No agent associated with UUID 6f4ed428-8de6-460e-9e19-6e6173776550"
    }


def test_post_measurement_tag_not_found(api_client_sync, monkeypatch):
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = api_client_sync.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "tag": "toto",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": "No agent associated with tag toto"}


def test_post_measurement_agent_multiple_definition(
    api_client_sync, agent, monkeypatch
):
    override(api_client_sync, get_redis, fake_redis_factory(agent=agent))
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = api_client_sync.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "tag": "test",
                    "target_file": "test.csv",
                },
                {
                    "uuid": str(agent.uuid),
                    "target_file": "test.csv",
                },
            ],
        },
    )
    assert response.status_code == 400
    assert response.json() == {"detail": f"Multiple definition of agent `{agent.uuid}`"}


def test_post_measurement_target_file_not_found(api_client_sync, agent, monkeypatch):
    override(api_client_sync, get_storage, fake_storage_factory([]))
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = api_client_sync.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": str(agent.uuid),
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 404


# --- GET /api/measurements/{measurement_uuid} ---


def test_get_measurement_by_uuid(api_client_sync, monkeypatch):
    user = "test"
    measurement_uuid = str(uuid.uuid4())
    start_time = datetime.now().isoformat()
    end_time = datetime.now().isoformat()
    agent = {
        "uuid": str(uuid.uuid4()),
        "target_file": "test.csv",
        "probing_rate": 100,
        "probing_statistics": {
            "1:10:0": {
                "probes_read": 240,
                "packets_sent": 240,
                "packets_failed": 0,
                "filtered_low_ttl": 0,
                "filtered_high_ttl": 0,
                "filtered_prefix_excl": 0,
                "filtered_prefix_not_incl": 0,
                "packets_received": 72,
                "packets_received_invalid": 0,
            }
        },
        "tool_parameters": {
            "initial_source_port": 24000,
            "destination_port": 34334,
            "flow_mapper": "IntervalFlowMapper",
            "flow_mapper_kwargs": {},
            "max_round": 5,
            "n_flow_ids": 6,
            "global_max_ttl": 255,
            "global_min_ttl": 0,
        },
        "agent_parameters": {
            "user": "all",
            "version": "0.0.0",
            "hostname": "test",
            "ipv4_address": "1.2.3.4",
            "ipv6_address": "::1234",
            "min_ttl": 1,
            "max_probing_rate": 200,
            "agent_tags": ["all"],
        },
        "state": "finished",
    }
    measurement = {
        "uuid": measurement_uuid,
        "user": user,
        "tool": "diamond-miner",
        "tags": ["test"],
        "state": "finished",
        "start_time": start_time,
        "end_time": end_time,
    }
    files = [
        {
            "key": "test",
            "size": 42,
            "content": "1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20",
            "last_modified": "test",
            "metadata": None,
        }
    ]

    monkeypatch.setattr(Agents, "all", async_mock([agent]))
    monkeypatch.setattr(Measurements, "get", async_mock(measurement))
    override(api_client_sync, get_storage, fake_storage_factory(files))

    response = api_client_sync.get(f"/api/measurements/{measurement_uuid}")
    assert response.json() == {
        "uuid": measurement_uuid,
        "state": "finished",
        "tool": "diamond-miner",
        "agents": [
            {
                "uuid": agent["uuid"],
                "state": "finished",
                "specific": {
                    "target_file": "test.csv",
                    "target_file_content": [
                        "1.1.1.0/24,icmp,2,32",
                        "2.2.2.0/24,udp,5,20",
                    ],
                    "probing_rate": 100,
                    "tool_parameters": {
                        "initial_source_port": 24000,
                        "destination_port": 34334,
                        "flow_mapper": "IntervalFlowMapper",
                        "flow_mapper_kwargs": {},
                        "max_round": 5,
                        "n_flow_ids": 6,
                        "global_max_ttl": 255,
                        "global_min_ttl": 0,
                    },
                },
                "parameters": {
                    "version": "0.0.0",
                    "hostname": "test",
                    "ipv4_address": "1.2.3.4",
                    "ipv6_address": "::1234",
                    "min_ttl": 1,
                    "max_probing_rate": 200,
                    "agent_tags": ["all"],
                },
                "probing_statistics": [
                    {
                        "round": "1:10:0",
                        "statistics": {
                            "probes_read": 240,
                            "packets_sent": 240,
                            "packets_failed": 0,
                            "filtered_low_ttl": 0,
                            "filtered_high_ttl": 0,
                            "filtered_prefix_excl": 0,
                            "filtered_prefix_not_incl": 0,
                            "packets_received": 72,
                            "packets_received_invalid": 0,
                        },
                    }
                ],
            }
        ],
        "tags": ["test"],
        "start_time": start_time,
        "end_time": end_time,
    }


def test_get_measurement_by_uuid_waiting(api_client_sync, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    start_time = datetime.now().isoformat()
    end_time = datetime.now().isoformat()
    user = "test"
    agent = {
        "uuid": str(uuid.uuid4()),
        "target_file": "test.csv",
        "probing_rate": None,
        "probing_statistics": {
            "1:10:0": {
                "probes_read": 240,
                "packets_sent": 240,
                "packets_failed": 0,
                "filtered_low_ttl": 0,
                "filtered_high_ttl": 0,
                "filtered_prefix_excl": 0,
                "filtered_prefix_not_incl": 0,
                "packets_received": 72,
                "packets_received_invalid": 0,
            }
        },
        "tool_parameters": {
            "initial_source_port": 24000,
            "destination_port": 34334,
            "flow_mapper": "IntervalFlowMapper",
            "flow_mapper_kwargs": {},
            "max_round": 5,
            "n_flow_ids": 6,
            "global_max_ttl": 255,
            "global_min_ttl": 0,
        },
        "agent_parameters": {
            "user": "all",
            "version": "0.0.0",
            "hostname": "test",
            "ipv4_address": "1.2.3.4",
            "ipv6_address": "::1234",
            "min_ttl": 1,
            "max_probing_rate": 200,
            "agent_tags": ["all"],
        },
        "state": "finished",
    }
    measurement = {
        "uuid": measurement_uuid,
        "user": user,
        "tool": "diamond-miner",
        "tags": ["test"],
        "state": None,
        "start_time": start_time,
        "end_time": end_time,
    }
    files = [
        {
            "key": "test",
            "size": 42,
            "content": "1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20",
            "last_modified": "test",
            "metadata": None,
        }
    ]

    override(
        api_client_sync,
        get_redis,
        fake_redis_factory(measurement_state=MeasurementState.Waiting),
    )
    override(api_client_sync, get_storage, fake_storage_factory(files))
    monkeypatch.setattr(Agents, "all", async_mock([agent]))
    monkeypatch.setattr(Measurements, "get", async_mock(measurement))

    response = api_client_sync.get(f"/api/measurements/{measurement_uuid}")
    assert response.json() == {
        "uuid": measurement_uuid,
        "state": "waiting",
        "tool": "diamond-miner",
        "agents": [
            {
                "uuid": agent["uuid"],
                "state": "waiting",
                "specific": {
                    "target_file": "test.csv",
                    "target_file_content": [
                        "1.1.1.0/24,icmp,2,32",
                        "2.2.2.0/24,udp,5,20",
                    ],
                    "probing_rate": None,
                    "tool_parameters": {
                        "initial_source_port": 24000,
                        "destination_port": 34334,
                        "flow_mapper": "IntervalFlowMapper",
                        "flow_mapper_kwargs": {},
                        "max_round": 5,
                        "n_flow_ids": 6,
                        "global_max_ttl": 255,
                        "global_min_ttl": 0,
                    },
                },
                "parameters": {
                    "version": "0.0.0",
                    "hostname": "test",
                    "ipv4_address": "1.2.3.4",
                    "ipv6_address": "::1234",
                    "min_ttl": 1,
                    "max_probing_rate": 200,
                    "agent_tags": ["all"],
                },
                "probing_statistics": [
                    {
                        "round": "1:10:0",
                        "statistics": {
                            "probes_read": 240,
                            "packets_sent": 240,
                            "packets_failed": 0,
                            "filtered_low_ttl": 0,
                            "filtered_high_ttl": 0,
                            "filtered_prefix_excl": 0,
                            "filtered_prefix_not_incl": 0,
                            "packets_received": 72,
                            "packets_received_invalid": 0,
                        },
                    }
                ],
            }
        ],
        "tags": ["test"],
        "start_time": start_time,
        "end_time": end_time,
    }


def test_get_measurement_by_uuid_not_found(api_client_sync, monkeypatch):
    monkeypatch.setattr(Measurements, "get", async_mock(None))
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
    monkeypatch.setattr(Measurements, "get", async_mock({"uuid": "uuid"}))
    measurement_uuid = str(uuid.uuid4())
    response = api_client_sync.delete(f"/api/measurements/{measurement_uuid}")
    assert response.json() == {"uuid": measurement_uuid, "action": "canceled"}


def test_delete_measurement_by_uuid_not_found(api_client_sync, monkeypatch):
    monkeypatch.setattr(Measurements, "get", async_mock(None))
    measurement_uuid = str(uuid.uuid4())
    response = api_client_sync.delete(f"/api/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_delete_measurement_by_uuid_already_finished(api_client_sync, monkeypatch):
    override(api_client_sync, get_redis, fake_redis_factory())
    monkeypatch.setattr(Measurements, "get", async_mock({"uuid": "uuid"}))
    measurement_uuid = str(uuid.uuid4())
    response = api_client_sync.delete(f"/api/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement already finished"}


# --- GET /api/measurements/{measurement_uuid}/{agent_uuid} ---


def test_get_measurement_results(api_client_sync, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())
    measurement = {
        "uuid": measurement_uuid,
        "user": "test",
        "tool": "diamond-miner",
        "tags": ["test"],
        "start_time": datetime.now().isoformat(),
        "end_time": datetime.now().isoformat(),
    }
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

    monkeypatch.setattr(Measurements, "get", async_mock(measurement))
    monkeypatch.setattr(Agents, "get", async_mock({"state": "finished"}))
    monkeypatch.setattr(Replies, "exists", async_mock(True))
    monkeypatch.setattr(Replies, "all", async_mock(results))
    monkeypatch.setattr(Replies, "all_count", async_mock(1))

    response = api_client_sync.get(
        f"/api/results/{measurement_uuid}/{agent_uuid}/replies/0.0.0.0"
    )
    assert response.json() == {
        "count": 1,
        "previous": None,
        "next": None,
        "results": results,
    }


def test_get_measurement_results_table_not_exists(api_client_sync, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())
    measurement = {
        "uuid": measurement_uuid,
        "user": "test",
        "tool": "diamond-miner",
        "tags": ["test"],
        "start_time": datetime.now().isoformat(),
        "end_time": datetime.now().isoformat(),
    }

    monkeypatch.setattr(Measurements, "get", async_mock(measurement))
    monkeypatch.setattr(Agents, "get", async_mock({"state": "finished"}))
    monkeypatch.setattr(Replies, "exists", async_mock(0))

    response = api_client_sync.get(
        f"/api/results/{measurement_uuid}/{agent_uuid}/replies/0.0.0.0"
    )
    assert response.json() == {
        "count": 0,
        "previous": None,
        "next": None,
        "results": [],
    }


def test_get_measurement_results_not_finished(api_client_sync, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())
    measurement = {
        "uuid": measurement_uuid,
        "user": "test",
        "tool": "diamond-miner",
        "tags": ["test"],
        "start_time": datetime.now().isoformat(),
        "end_time": datetime.now().isoformat(),
    }

    monkeypatch.setattr(Measurements, "get", async_mock(measurement))
    monkeypatch.setattr(Agents, "get", async_mock({"state": "ongoing"}))

    response = api_client_sync.get(
        f"/api/results/{measurement_uuid}/{agent_uuid}/replies/0.0.0.0"
    )
    assert response.status_code == 412


def test_get_measurement_results_no_agent(api_client_sync, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())
    measurement = {
        "uuid": measurement_uuid,
        "user": "test",
        "tool": "diamond-miner",
        "tags": ["test"],
        "start_time": datetime.now().isoformat(),
        "end_time": datetime.now().isoformat(),
    }

    monkeypatch.setattr(Measurements, "get", async_mock(measurement))
    monkeypatch.setattr(Agents, "get", async_mock(None))

    response = api_client_sync.get(
        f"/api/results/{measurement_uuid}/{agent_uuid}/replies/0.0.0.0"
    )
    assert response.status_code == 404
    assert response.json() == {
        "detail": (
            f"The agent `{agent_uuid}` "
            f"did not participate to measurement `{measurement_uuid}`"
        )
    }


def test_get_measurement_result_not_found(api_client_sync, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())
    monkeypatch.setattr(Measurements, "get", async_mock(None))
    response = api_client_sync.get(
        f"/api/results/{measurement_uuid}/{agent_uuid}/replies/0.0.0.0"
    )
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_get_measurement_results_invalid_measurement_uuid(api_client_sync):
    measurement_uuid = "test"
    agent_uuid = str(uuid.uuid4())
    response = api_client_sync.get(
        f"/api/results/{measurement_uuid}/{agent_uuid}/replies/0.0.0.0"
    )
    assert response.status_code == 422


def test_get_measurement_results_invalid_agent_uuid(api_client_sync):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = "test"
    response = api_client_sync.get(
        f"/api/results/{measurement_uuid}/{agent_uuid}/replies/0.0.0.0"
    )
    assert response.status_code == 422
