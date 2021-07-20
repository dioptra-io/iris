import uuid
from datetime import datetime

import pytest

import iris.commons.database.agents
import iris.commons.database.measurements
import iris.commons.database.results
from iris.api.measurements import verify_quota
from iris.api.security import get_current_active_user

from ..conftest import override_get_current_active_user

# --- GET /api/measurements ---


def test_get_measurements_empty(client, monkeypatch):
    async def all(self, user, offset, limit, tag=None):
        return []

    async def all_count(self, *args, **kwargs):
        return 0

    monkeypatch.setattr(iris.commons.database.measurements.Measurements, "all", all)
    monkeypatch.setattr(
        iris.commons.database.measurements.Measurements, "all_count", all_count
    )

    response = client.get("/api/measurements")
    assert response.json() == {
        "count": 0,
        "next": None,
        "previous": None,
        "results": [],
    }


def test_get_measurements(client, monkeypatch):
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

    async def all_count(self, *args, **kwargs):
        return 3

    monkeypatch.setattr(iris.commons.database.measurements.Measurements, "all", all)
    monkeypatch.setattr(
        iris.commons.database.measurements.Measurements, "all_count", all_count
    )

    # No (offset, limit)
    response = client.get("/api/measurements")
    assert response.json() == {
        "count": 3,
        "next": None,
        "previous": None,
        "results": measurements,
    }

    # All inclusive (0, 100)
    response = client.get("/api/measurements?offset=0&limit=100")
    assert response.json() == {
        "count": 3,
        "next": None,
        "previous": None,
        "results": measurements,
    }

    # First result (0, 1)
    response = client.get("/api/measurements?offset=0&limit=1")
    assert response.json() == {
        "count": 3,
        "next": "http://testserver/api/measurements/?limit=1&offset=1",
        "previous": None,
        "results": measurements[0:1],
    }

    # Middle result (1, 1)
    response = client.get("/api/measurements?offset=1&limit=1")
    assert response.json() == {
        "count": 3,
        "next": "http://testserver/api/measurements/?limit=1&offset=2",
        "previous": "http://testserver/api/measurements/?limit=1",
        "results": measurements[1:2],
    }

    # Last result (2, 1)
    response = client.get("/api/measurements?offset=2&limit=1")
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


def test_post_measurement_diamond_miner(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 201


def test_post_measurement_diamond_miner_quota_exceeded(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.dependency_overrides[get_current_active_user] = lambda: {
        "uuid": "test",
        "username": "test",
        "email": "test@test",
        "is_active": True,
        "is_admin": False,
        "quota": 0,
        "register_date": "date",
        "ripe_account": None,
        "ripe_key": None,
    }

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 403

    # Reset back the override
    client.app.dependency_overrides[
        get_current_active_user
    ] = override_get_current_active_user


def test_post_measurement_diamond_miner_invalid_prefix_length(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/25,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 403


def test_post_measurement_yarrp(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "yarrp",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 201


def test_post_measurement_yarrp_quota_exceeded(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.dependency_overrides[get_current_active_user] = lambda: {
        "uuid": "test",
        "username": "test",
        "email": "test@test",
        "is_active": True,
        "is_admin": False,
        "quota": 0,
        "register_date": "date",
        "ripe_account": None,
        "ripe_key": None,
    }

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "yarrp",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 403

    # Reset back the override
    client.app.dependency_overrides[
        get_current_active_user
    ] = override_get_current_active_user


def test_post_measurement_ping(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.8,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "ping",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 201


def test_post_measurement_ping_udp(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,udp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "ping",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 403


def test_post_measurement_ping_quota_exceeded(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/24,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.dependency_overrides[get_current_active_user] = lambda: {
        "uuid": "test",
        "username": "test",
        "email": "test@test",
        "is_active": True,
        "is_admin": False,
        "quota": 0,
        "register_date": "date",
        "ripe_account": None,
        "ripe_key": None,
    }

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 403

    # Reset back the override
    client.app.dependency_overrides[
        get_current_active_user
    ] = override_get_current_active_user


def test_post_measurement_agent_tag(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "agent_tag": "test",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 201


def test_post_measurement_no_uuid_or_tag(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
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
    assert response.status_code == 412
    assert response.json() == {
        "detail": "Either an agent UUID or an agent tag must be provided"
    }


def test_post_measurement_tag_and_uuid(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776550",
                    "agent_tag": "test",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 412
    assert response.json() == {
        "detail": "Either an agent UUID or an agent tag must be provided"
    }


def test_post_measurement_with_agent_not_found(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
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
    assert response.json() == {"detail": "Agent not found"}


def test_post_measurement_tag_not_found(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "agent_tag": "toto",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 404
    assert response.json() == {"detail": "No agent associated with this tag"}


def test_post_measurement_agent_multiple_definition(client, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test.csv",
                "size": 42,
                "content": "8.8.8.0/23,icmp,2,32",
                "last_modified": "test",
            }

    class FakeSend(object):
        def send(*args, **kwargs):
            pass

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", FakeSend())

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "agent_tag": "test",
                    "target_file": "test.csv",
                },
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                },
            ],
        },
    )
    assert response.status_code == 403
    assert response.json() == {
        "detail": "Multiple definition of agent `6f4ed428-8de6-460e-9e19-6e6173776552`"
    }


def test_post_measurement_target_file_not_found(client, monkeypatch):
    class FakeStorage(object):
        async def get_file(*args, **kwargs):
            raise Exception

    client.app.storage = FakeStorage()
    monkeypatch.setattr("iris.api.measurements.hook", lambda x, y: None)

    response = client.post(
        "/api/measurements/",
        json={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "target_file": "test.csv",
                }
            ],
        },
    )
    assert response.status_code == 404


# --- GET /api/measurements/{measurement_uuid} ---


def test_get_measurement_by_uuid(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    user = "test"
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
            "max_round": 5,
            "flow_mapper": "IntervalFlowMapper",
            "flow_mapper_kwargs": {},
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

    start_time = datetime.now().isoformat()
    end_time = datetime.now().isoformat()

    async def all_agents(self, measurement_uuid):
        return [agent]

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": user,
            "tool": "diamond-miner",
            "tags": ["test"],
            "state": "finished",
            "start_time": start_time,
            "end_time": end_time,
        }

    monkeypatch.setattr(
        iris.commons.database.agents.Agents,
        "all",
        all_agents,
    )
    monkeypatch.setattr(
        iris.commons.database.measurements.Measurements, "get", get_measurements
    )

    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test",
                "size": 42,
                "content": "1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20",
                "last_modified": "test",
                "metadata": None,
            }

    client.app.storage = FakeStorage()

    response = client.get(f"/api/measurements/{measurement_uuid}")
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
                        "max_round": 5,
                        "flow_mapper": "IntervalFlowMapper",
                        "flow_mapper_kwargs": {},
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


def test_get_measurement_by_uuid_waiting(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
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
            "max_round": 5,
            "flow_mapper": "IntervalFlowMapper",
            "flow_mapper_kwargs": {},
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

    start_time = datetime.now().isoformat()
    end_time = datetime.now().isoformat()

    async def all_agents(self, measurement_uuid):
        return [agent]

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": user,
            "tool": "diamond-miner",
            "tags": ["test"],
            "start_time": start_time,
            "end_time": end_time,
        }

    async def get_agents(self, uuid, user="all"):
        return {
            "uuid": agent["uuid"],
            "last_used": datetime.now().isoformat(),
        }

    class FakeRedis(object):
        async def get_measurement_state(*args, **kwargs):
            return "waiting"

    client.app.redis = FakeRedis()

    monkeypatch.setattr(
        iris.commons.database.agents.Agents,
        "all",
        all_agents,
    )
    monkeypatch.setattr(
        iris.commons.database.measurements.Measurements, "get", get_measurements
    )

    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test",
                "size": 42,
                "content": "1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20",
                "last_modified": "test",
                "metadata": None,
            }

    client.app.storage = FakeStorage()

    response = client.get(f"/api/measurements/{measurement_uuid}")
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
                        "max_round": 5,
                        "flow_mapper": "IntervalFlowMapper",
                        "flow_mapper_kwargs": {},
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


def test_get_measurement_by_uuid_not_found(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())

    async def get(self, username, measurement_uuid):
        return None

    monkeypatch.setattr(iris.commons.database.measurements.Measurements, "get", get)

    response = client.get(f"/api/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_get_measurement_by_uuid_invalid_input(client):
    response = client.get("/api/measurements/test")
    assert response.status_code == 422


# -- DELETE /api/measurements/{measurement_uuid}/{agent_uuid} ---


def test_delete_measurement_by_uuid(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())

    class FakeRedis(object):
        async def get_measurement_state(*args, **kwargs):
            return "ongoing"

        async def set_measurement_state(*args, **kwargs):
            pass

    async def get(self, username, measurement_uuid):
        return {"uuid": "uuid"}

    client.app.redis = FakeRedis()
    monkeypatch.setattr(iris.commons.database.measurements.Measurements, "get", get)

    response = client.delete(f"/api/measurements/{measurement_uuid}")
    assert response.json() == {"uuid": measurement_uuid, "action": "canceled"}


def test_delete_measurement_by_uuid_not_found(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())

    async def get(self, username, measurement_uuid):
        return None

    monkeypatch.setattr(iris.commons.database.measurements.Measurements, "get", get)

    response = client.delete(f"/api/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_delete_measurement_by_uuid_already_finished(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())

    class FakeRedis(object):
        async def get_measurement_state(*args, **kwargs):
            return None

    async def get(self, username, measurement_uuid):
        return {"uuid": "uuid"}

    client.app.redis = FakeRedis()
    monkeypatch.setattr(iris.commons.database.measurements.Measurements, "get", get)

    response = client.delete(f"/api/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement already finished"}


# --- GET /api/measurements/{measurement_uuid}/{agent_uuid} ---


def test_get_measurement_results(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    results = [
        {
            "probe_protocol": 0,
            "probe_src_addr": "::ffff:ac12:b",
            "probe_dst_addr": "::ffff:84e3:7b81",
            "probe_src_port": 24000,
            "probe_dst_port": 34334,
            "probe_ttl": 78,
            "quoted_ttl": 9,
            "reply_src_addr": "::ffff:869d:fe0a",
            "reply_protocol": 17,
            "reply_icmp_type": 11,
            "reply_icmp_code": 0,
            "reply_ttl": 37,
            "reply_size": 56,
            "reply_mpls_labels": [1],
            "rtt": 1280.2,
            "round": 1,
        }
    ]

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "tool": "diamond-miner",
            "tags": ["test"],
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    async def get_agents_results(self, measurement_uuid, agent_uuid):
        return {"state": "finished"}

    async def all_measurement_results(self, offset, limit):
        return results

    async def all_measurement_results_count(self):
        return 1

    async def measurement_results_exists(self):
        return 1

    monkeypatch.setattr(
        iris.commons.database.measurements.Measurements, "get", get_measurements
    )
    monkeypatch.setattr(
        iris.commons.database.agents.Agents,
        "get",
        get_agents_results,
    )

    monkeypatch.setattr(
        iris.commons.database.measurement_results.InsertResults,
        "exists",
        measurement_results_exists,
    )

    monkeypatch.setattr(
        iris.commons.database.measurement_results.InsertResults,
        "all",
        all_measurement_results,
    )
    monkeypatch.setattr(
        iris.commons.database.measurement_results.InsertResults,
        "all_count",
        all_measurement_results_count,
    )

    response = client.get(f"/api/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.json() == {
        "count": 1,
        "previous": None,
        "next": None,
        "results": results,
    }


def test_get_measurement_results_table_not_exists(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "tool": "diamond-miner",
            "tags": ["test"],
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    async def get_agents_results(self, measurement_uuid, agent_uuid):
        return {"state": "finished"}

    async def measurement_results_exists(self):
        return 0

    monkeypatch.setattr(
        iris.commons.database.measurements.Measurements, "get", get_measurements
    )
    monkeypatch.setattr(
        iris.commons.database.agents.Agents,
        "get",
        get_agents_results,
    )

    monkeypatch.setattr(
        iris.commons.database.measurement_results.InsertResults,
        "exists",
        measurement_results_exists,
    )

    response = client.get(f"/api/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.json() == {
        "count": 0,
        "previous": None,
        "next": None,
        "results": [],
    }


def test_get_measurement_results_not_finished(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "tool": "diamond-miner",
            "tags": ["test"],
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    async def get_agents_results(self, measurement_uuid, agent_uuid):
        return {"state": "ongoing"}

    monkeypatch.setattr(
        iris.commons.database.measurements.Measurements, "get", get_measurements
    )
    monkeypatch.setattr(
        iris.commons.database.agents.Agents,
        "get",
        get_agents_results,
    )

    response = client.get(f"/api/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 412


def test_get_measurement_results_no_agent(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "tool": "diamond-miner",
            "tags": ["test"],
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    async def get_agents_results(self, measurement_uuid, agent_uuid):
        return None

    monkeypatch.setattr(
        iris.commons.database.measurements.Measurements, "get", get_measurements
    )
    monkeypatch.setattr(
        iris.commons.database.agents.Agents,
        "get",
        get_agents_results,
    )

    response = client.get(f"/api/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 404
    assert response.json() == {
        "detail": (
            f"The agent `{agent_uuid}` "
            f"did not participate to measurement `{measurement_uuid}`"
        )
    }


def test_get_measurement_result_not_found(client, monkeypatch):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get(self, username, measurement_uuid):
        return None

    monkeypatch.setattr(iris.commons.database.measurements.Measurements, "get", get)

    response = client.get(f"/api/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_get_measurement_results_invalid_measurement_uuid(client):
    measurement_uuid = "test"
    agent_uuid = str(uuid.uuid4())
    response = client.get(f"/api/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 422


def test_get_measurement_results_invalid_agent_uuid(client):
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = "test"
    response = client.get(f"/api/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 422
