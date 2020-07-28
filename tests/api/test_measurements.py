"""Test `measurements` section."""

import iris.commons.database
import uuid

from datetime import datetime


# --- GET /v0/measurements ---


def test_get_measurements_empty(client, monkeypatch):
    """Test get all measurements when no measurement in database."""

    async def all(self, username):
        return []

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "all", all)

    response = client.get("/v0/measurements")
    assert response.json() == {"count": 0, "results": []}


def test_get_measurements(client, monkeypatch):
    """Test get all measurements."""

    measurement_uuid = str(uuid.uuid4())

    async def all(self, username):
        return [measurement_uuid]

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "all", all)

    response = client.get("/v0/measurements")
    assert response.json() == {
        "count": 1,
        "results": [{"uuid": measurement_uuid, "status": "finished"}],
    }


# --- GET /v0/measurements/{measurement_uuid} ---


def test_get_measurement_by_uuid(client, monkeypatch):
    """Test get measurements UUID."""

    measurement_uuid = str(uuid.uuid4())
    user = "test"
    agents = [str(uuid.uuid4())]
    target_file_key = "test.txt"
    protocol = "udp"
    destination_port = 33434
    min_ttl = 2
    max_ttl = 30
    start_time = datetime.now().isoformat()
    end_time = datetime.now().isoformat()

    async def get(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": user,
            "agents": agents,
            "target_file_key": target_file_key,
            "protocol": protocol,
            "destination_port": destination_port,
            "min_ttl": min_ttl,
            "max_ttl": max_ttl,
            "start_time": start_time,
            "end_time": end_time,
        }

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "get", get)

    response = client.get(f"/v0/measurements/{measurement_uuid}")
    assert response.json() == {
        "uuid": measurement_uuid,
        "status": "finished",
        "agents": agents,
        "target_file_key": target_file_key,
        "protocol": protocol,
        "destination_port": destination_port,
        "min_ttl": min_ttl,
        "max_ttl": max_ttl,
        "start_time": start_time,
        "end_time": end_time,
    }


def test_get_measurement_by_uuid_not_found(client, monkeypatch):
    """Test get measurements UUID that don't exist."""

    measurement_uuid = str(uuid.uuid4())

    async def get(self, username, measurement_uuid):
        return None

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "get", get)

    response = client.get(f"/v0/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


# --- /v0/measurements/{measurement_uuid}/{agent_uuid} ---


def test_get_measurement_result(client, monkeypatch):
    """Test get measurements results."""

    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    results = [
        {
            "source_ip": "0.0.0.0",
            "destination_prefix": "0.0.0.0",
            "destination_ip": "0.0.0.0",
            "reply_ip": "0.0.0.0",
            "protocol": "udp",
            "source_port": 0,
            "destination_port": 33434,
            "ttl": 0,
            "ttl_check": 0,
            "type": 11,
            "code": 0,
            "rtt": 0.0,
            "reply_ttl": 0,
            "reply_size": 0,
            "round": 1,
        }
    ]

    async def get(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "agents": [str(uuid.uuid4())],
            "target_file_key": "test.txt",
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 30,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    async def get_results(self):
        return results

    class FakeClient(object):
        def __init__(self, *args, **kwargs):
            pass

        async def execute(*args, **kwargs):
            return [[1]]

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "get", get)
    monkeypatch.setattr(iris.api.results.MeasurementResults, "get_results", get_results)
    monkeypatch.setattr(iris.api.measurements.aioch, "Client", FakeClient)

    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.json() == {
        "count": 1,
        "previous": None,
        "next": None,
        "results": results,
    }


def test_get_measurement_no_result(client, monkeypatch):
    """Test get measurements results that don't exist."""

    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "agents": [str(uuid.uuid4())],
            "target_file_key": "test.txt",
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 30,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    class FakeClient(object):
        def __init__(self, *args, **kwargs):
            pass

        async def execute(*args, **kwargs):
            return [[0]]

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "get", get)
    monkeypatch.setattr(iris.api.measurements.aioch, "Client", FakeClient)

    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 404
    assert response.json() == {
        "detail": (
            f"The agent `{agent_uuid}` "
            f"did not participate to measurement `{measurement_uuid}`"
        )
    }


def test_get_measurement_result_not_found(client, monkeypatch):
    """Test get measurements result that don't exist."""

    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get(self, username, measurement_uuid):
        return None

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "get", get)

    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}
