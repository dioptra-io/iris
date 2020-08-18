"""Test `measurements` section."""

import iris.commons.database
import uuid

from datetime import datetime


# --- GET /v0/measurements ---


def test_get_measurements_empty(client, monkeypatch):
    """Test get all measurements when no measurement in database."""

    async def all(self, user, offset, limit):
        return []

    async def all_count(self):
        return 0

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "all", all)
    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurements, "all_count", all_count
    )

    response = client.get("/v0/measurements")
    assert response.json() == {
        "count": 0,
        "next": None,
        "previous": None,
        "results": [],
    }


def test_get_measurements(client, monkeypatch):
    """Test get all measurements."""

    measurements = [
        {
            "uuid": str(uuid.uuid4()),
            "state": "finished",
            "targets_file_key": "test.txt",
            "full": False,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        },
        {
            "uuid": str(uuid.uuid4()),
            "state": "finished",
            "targets_file_key": "test.txt",
            "full": False,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        },
        {
            "uuid": str(uuid.uuid4()),
            "state": "finished",
            "targets_file_key": "test.txt",
            "full": False,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        },
    ]

    measurements = sorted(measurements, key=lambda x: x["start_time"], reverse=True)

    async def all(self, user, offset, limit):
        return [
            {
                "uuid": measurements[0]["uuid"],
                "targets_file_key": measurements[0]["targets_file_key"],
                "full": measurements[0]["full"],
                "start_time": measurements[0]["start_time"],
                "end_time": measurements[0]["end_time"],
            },
            {
                "uuid": measurements[1]["uuid"],
                "targets_file_key": measurements[1]["targets_file_key"],
                "full": measurements[1]["full"],
                "start_time": measurements[1]["start_time"],
                "end_time": measurements[1]["end_time"],
            },
            {
                "uuid": measurements[2]["uuid"],
                "targets_file_key": measurements[2]["targets_file_key"],
                "full": measurements[2]["full"],
                "start_time": measurements[2]["start_time"],
                "end_time": measurements[2]["end_time"],
            },
        ][
            offset : offset + limit  # noqa : E203
        ]

    async def all_count(self):
        return 3

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "all", all)
    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurements, "all_count", all_count
    )

    # No (offset, limit)
    response = client.get("/v0/measurements")
    assert response.json() == {
        "count": 3,
        "next": None,
        "previous": None,
        "results": measurements,
    }

    # All inclusive (0, 100)
    response = client.get("/v0/measurements?offset=0&limit=100")
    assert response.json() == {
        "count": 3,
        "next": None,
        "previous": None,
        "results": measurements,
    }

    # First result (0, 1)
    response = client.get("/v0/measurements?offset=0&limit=1")
    assert response.json() == {
        "count": 3,
        "next": "http://testserver/v0/measurements/?limit=1&offset=1",
        "previous": None,
        "results": measurements[0:1],
    }

    # Middle result (1, 1)
    response = client.get("/v0/measurements?offset=1&limit=1")
    assert response.json() == {
        "count": 3,
        "next": "http://testserver/v0/measurements/?limit=1&offset=2",
        "previous": "http://testserver/v0/measurements/?limit=1",
        "results": measurements[1:2],
    }

    # Last result (2, 1)
    response = client.get("/v0/measurements?offset=2&limit=1")
    assert response.json() == {
        "count": 3,
        "next": None,
        "previous": "http://testserver/v0/measurements/?limit=1&offset=1",
        "results": measurements[2:3],
    }


# --- GET /v0/measurements/{measurement_uuid} ---


def test_get_measurement_by_uuid(client, monkeypatch):
    """Test get measurement by UUID."""

    measurement_uuid = str(uuid.uuid4())
    user = "test"
    agent = {
        "uuid": str(uuid.uuid4()),
        "min_ttl": 2,
        "max_ttl": 30,
        "probing_rate": None,
        "state": "finished",
    }
    targets_file_key = "test.txt"
    protocol = "udp"
    destination_port = 33434
    min_ttl = 2
    max_ttl = 30
    max_round = 10
    start_time = datetime.now().isoformat()
    end_time = datetime.now().isoformat()

    async def all_agents_specific(self, measurement_uuid):
        return [agent]

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": user,
            "targets_file_key": targets_file_key,
            "full": False,
            "protocol": protocol,
            "destination_port": destination_port,
            "min_ttl": min_ttl,
            "max_ttl": max_ttl,
            "max_round": max_round,
            "start_time": start_time,
            "end_time": end_time,
        }

    async def get_agents(self, uuid, user="all"):
        return {
            "uuid": agent["uuid"],
            "user": "all",
            "version": "0.0.0",
            "hostname": "test",
            "ip_address": "0.0.0.0",
            "probing_rate": 0,
            "buffer_sniffer_size": 0,
            "inf_born": 0,
            "sup_born": 0,
            "ips_per_subnet": 0,
            "pfring": False,
            "last_used": datetime.now().isoformat(),
        }

    monkeypatch.setattr(
        iris.commons.database.DatabaseAgentsSpecific, "all", all_agents_specific,
    )
    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurements, "get", get_measurements
    )
    monkeypatch.setattr(iris.commons.database.DatabaseAgents, "get", get_agents)

    response = client.get(f"/v0/measurements/{measurement_uuid}")
    assert response.json() == {
        "uuid": measurement_uuid,
        "state": "finished",
        "agents": [
            {
                "uuid": agent["uuid"],
                "state": "finished",
                "specific": {"min_ttl": 2, "max_ttl": 30, "probing_rate": 0},
                "parameters": {
                    "version": "0.0.0",
                    "hostname": "test",
                    "ip_address": "0.0.0.0",
                    "probing_rate": 0,
                },
            }
        ],
        "targets_file_key": targets_file_key,
        "full": False,
        "protocol": protocol,
        "destination_port": destination_port,
        "min_ttl": min_ttl,
        "max_ttl": max_ttl,
        "max_round": max_round,
        "start_time": start_time,
        "end_time": end_time,
    }


def test_get_measurement_by_uuid_custom_probing_rate(client, monkeypatch):
    """Test get measurement by UUID."""

    measurement_uuid = str(uuid.uuid4())
    user = "test"
    agent = {
        "uuid": str(uuid.uuid4()),
        "min_ttl": 2,
        "max_ttl": 30,
        "probing_rate": 100,
        "state": "finished",
    }
    targets_file_key = "test.txt"
    protocol = "udp"
    destination_port = 33434
    min_ttl = 2
    max_ttl = 30
    max_round = 10
    start_time = datetime.now().isoformat()
    end_time = datetime.now().isoformat()

    async def all_agents_specific(self, measurement_uuid):
        return [agent]

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": user,
            "targets_file_key": targets_file_key,
            "full": False,
            "protocol": protocol,
            "destination_port": destination_port,
            "min_ttl": min_ttl,
            "max_ttl": max_ttl,
            "max_round": max_round,
            "start_time": start_time,
            "end_time": end_time,
        }

    async def get_agents(self, uuid, user="all"):
        return {
            "uuid": agent["uuid"],
            "user": "all",
            "version": "0.0.0",
            "hostname": "test",
            "ip_address": "0.0.0.0",
            "probing_rate": 0,
            "buffer_sniffer_size": 0,
            "inf_born": 0,
            "sup_born": 0,
            "ips_per_subnet": 0,
            "pfring": False,
            "last_used": datetime.now().isoformat(),
        }

    monkeypatch.setattr(
        iris.commons.database.DatabaseAgentsSpecific, "all", all_agents_specific,
    )
    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurements, "get", get_measurements
    )
    monkeypatch.setattr(iris.commons.database.DatabaseAgents, "get", get_agents)

    response = client.get(f"/v0/measurements/{measurement_uuid}")
    assert response.json() == {
        "uuid": measurement_uuid,
        "state": "finished",
        "agents": [
            {
                "uuid": agent["uuid"],
                "state": "finished",
                "specific": {"min_ttl": 2, "max_ttl": 30, "probing_rate": 100},
                "parameters": {
                    "version": "0.0.0",
                    "hostname": "test",
                    "ip_address": "0.0.0.0",
                    "probing_rate": 0,
                },
            }
        ],
        "targets_file_key": targets_file_key,
        "full": False,
        "protocol": protocol,
        "destination_port": destination_port,
        "min_ttl": min_ttl,
        "max_ttl": max_ttl,
        "max_round": max_round,
        "start_time": start_time,
        "end_time": end_time,
    }


def test_get_measurement_by_uuid_not_found(client, monkeypatch):
    """Test get measurement by UUID that don't exist."""

    measurement_uuid = str(uuid.uuid4())

    async def get(self, username, measurement_uuid):
        return None

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "get", get)

    response = client.get(f"/v0/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_get_measurement_by_uuid_invalid_input(client):
    """Test get measurement by UUID with invalid input."""
    response = client.get("/v0/measurements/test")
    assert response.status_code == 422


# --- GET /v0/measurements/{measurement_uuid}/{agent_uuid} ---


def test_get_measurement_results(client, monkeypatch):
    """Test get measurement results."""

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

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "agents": [str(uuid.uuid4())],
            "targets_file_key": "test.txt",
            "full": False,
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 30,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    async def get_agents_specific_results(self, measurement_uuid, agent_uuid):
        return {"state": "finished"}

    async def all_measurement_results(self, offset, limit):
        return results

    async def all_measurement_results_count(self):
        return 1

    async def measurement_results_is_exists(self):
        return 1

    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurements, "get", get_measurements
    )
    monkeypatch.setattr(
        iris.commons.database.DatabaseAgentsSpecific,
        "get",
        get_agents_specific_results,
    )

    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurementResults,
        "is_exists",
        measurement_results_is_exists,
    )

    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurementResults, "all", all_measurement_results
    )
    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurementResults,
        "all_count",
        all_measurement_results_count,
    )

    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.json() == {
        "count": 1,
        "previous": None,
        "next": None,
        "results": results,
    }


def test_get_measurement_results_table_not_exists(client, monkeypatch):
    """Test get measurement results if the table not exists."""

    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "agents": [str(uuid.uuid4())],
            "targets_file_key": "test.txt",
            "full": False,
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 30,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    async def get_agents_specific_results(self, measurement_uuid, agent_uuid):
        return {"state": "finished"}

    async def measurement_results_is_exists(self):
        return 0

    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurements, "get", get_measurements
    )
    monkeypatch.setattr(
        iris.commons.database.DatabaseAgentsSpecific,
        "get",
        get_agents_specific_results,
    )

    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurementResults,
        "is_exists",
        measurement_results_is_exists,
    )

    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.json() == {
        "count": 0,
        "previous": None,
        "next": None,
        "results": [],
    }


def test_get_measurement_results_not_finished(client, monkeypatch):
    """Test get measurement results but the agent has not finished."""

    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "agents": [str(uuid.uuid4())],
            "targets_file_key": "test.txt",
            "full": False,
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 30,
            "max_round": 10,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    async def get_agents_specific_results(self, measurement_uuid, agent_uuid):
        return {"state": "ongoing"}

    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurements, "get", get_measurements
    )
    monkeypatch.setattr(
        iris.commons.database.DatabaseAgentsSpecific,
        "get",
        get_agents_specific_results,
    )

    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 412


def test_get_measurement_results_no_agent(client, monkeypatch):
    """Test get measurement results that don't exist."""

    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get_measurements(self, username, measurement_uuid):
        return {
            "uuid": measurement_uuid,
            "user": "test",
            "agents": [str(uuid.uuid4())],
            "targets_file_key": "test.txt",
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 30,
            "max_round": 10,
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }

    async def get_agents_specific_results(self, measurement_uuid, agent_uuid):
        return None

    monkeypatch.setattr(
        iris.commons.database.DatabaseMeasurements, "get", get_measurements
    )
    monkeypatch.setattr(
        iris.commons.database.DatabaseAgentsSpecific,
        "get",
        get_agents_specific_results,
    )

    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 404
    assert response.json() == {
        "detail": (
            f"The agent `{agent_uuid}` "
            f"did not participate to measurement `{measurement_uuid}`"
        )
    }


def test_get_measurement_result_not_found(client, monkeypatch):
    """Test get measurement results that don't exist."""

    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    async def get(self, username, measurement_uuid):
        return None

    monkeypatch.setattr(iris.commons.database.DatabaseMeasurements, "get", get)

    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_get_measurement_by_uuid_invalid_measurement_uuid(client):
    """Test get measurement results with invalid input."""
    measurement_uuid = "test"
    agent_uuid = str(uuid.uuid4())
    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 422


def test_get_measurement_by_uuid_invalid_agent_uuid(client):
    """Test get measurement results with invalid input."""
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = "test"
    response = client.get(f"/v0/measurements/{measurement_uuid}/{agent_uuid}")
    assert response.status_code == 422
