"""Test of commons database classes."""

import pytest
import uuid

from datetime import datetime
from iris.commons.database import (
    Database,
    DatabaseMeasurements,
    DatabaseAgents,
    DatabaseAgentsSpecific,
    DatabaseMeasurementResults,
)


class FakeConnection:
    hosts = [["127.0.0.1"]]


class FakeCient:
    connection = FakeConnection()


class FakeSession(object):
    def __init__(self, response):
        self.response = response
        self._client = FakeCient()

    async def execute(self, *args, **kwargs):
        return self.response

    async def disconnect(self):
        pass


@pytest.mark.asyncio
async def test_database(monkeypatch):
    session = FakeSession(response=None)

    assert await Database(session).create_datebase() is None
    assert await Database(session).drop_table("test") is None
    assert await Database(session).clean_table("test") is None
    assert await Database(session).disconnect() is None


@pytest.mark.asyncio
async def test_database_measurements(monkeypatch):
    session = FakeSession(response=None)
    assert await DatabaseMeasurements(session).create_table() is None
    assert await DatabaseMeasurements(session).create_table(drop=True) is None

    # Test of `.all_count() method`
    session = FakeSession(response=[(10,)])
    assert await DatabaseMeasurements(session).all_count() == 10

    measurement_uuid_1 = uuid.uuid4()
    fake_database_response_1 = (
        measurement_uuid_1,
        "admin",
        "key",
        0,
        "udp",
        33434,
        2,
        30,
        10,
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
        datetime.strptime("2020-01-02", "%Y-%m-%d"),
    )

    measurement_uuid_2 = uuid.uuid4()
    fake_database_response_2 = (
        measurement_uuid_2,
        "admin",
        "key",
        0,
        "udp",
        33434,
        2,
        30,
        10,
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
        None,
    )

    fake_formated_response_1 = {
        "uuid": str(measurement_uuid_1),
        "user": "admin",
        "targets_file_key": "key",
        "full": False,
        "protocol": "udp",
        "destination_port": 33434,
        "min_ttl": 2,
        "max_ttl": 30,
        "max_round": 10,
        "start_time": datetime.strptime("2020-01-01", "%Y-%m-%d").isoformat(),
        "end_time": datetime.strptime("2020-01-02", "%Y-%m-%d").isoformat(),
    }

    fake_formated_response_2 = {
        "uuid": str(measurement_uuid_2),
        "user": "admin",
        "targets_file_key": "key",
        "full": False,
        "protocol": "udp",
        "destination_port": 33434,
        "min_ttl": 2,
        "max_ttl": 30,
        "max_round": 10,
        "start_time": datetime.strptime("2020-01-01", "%Y-%m-%d").isoformat(),
        "end_time": None,
    }

    # Test of `.all() method`
    session = FakeSession(response=[fake_database_response_1, fake_database_response_2])
    assert await DatabaseMeasurements(session).all("admin", 0, 100) == [
        fake_formated_response_1,
        fake_formated_response_2,
    ]

    # Test of `.get() method`
    session = FakeSession(response=[fake_database_response_1])
    assert (
        await DatabaseMeasurements(session).get("admin", measurement_uuid_1)
        == fake_formated_response_1
    )
    session = FakeSession(response=[fake_database_response_2])
    assert (
        await DatabaseMeasurements(session).get("admin", measurement_uuid_2)
        == fake_formated_response_2
    )
    session = FakeSession(response=[])
    assert await DatabaseMeasurements(session).get("admin", measurement_uuid_1) is None

    parameters = {
        "measurement_uuid": measurement_uuid_1,
        "user": "admin",
        "targets_file_key": "key",
        "full": False,
        "protocol": "udp",
        "destination_port": 33434,
        "min_ttl": 2,
        "max_ttl": 30,
        "max_round": 10,
        "start_time": 1597829098,
    }

    # Test of `.register() method`
    session = FakeSession(response=None)
    assert await DatabaseMeasurements(session).register(parameters) is None

    # Test of `.stamp_end_time() method`
    session = FakeSession(response=None)
    assert (
        await DatabaseMeasurements(session).stamp_end_time("admin", measurement_uuid_1)
        is None
    )


@pytest.mark.asyncio
async def test_database_agents(monkeypatch):
    session = FakeSession(response=None)
    assert await DatabaseAgents(session).create_table() is None
    assert await DatabaseAgents(session).create_table(drop=True) is None

    agent_uuid = uuid.uuid4()
    fake_database_response = (
        agent_uuid,
        "all",
        "0.1.0",
        "hostname",
        "1.2.3.4",
        1000,
        10000000,
        0,
        32,
        6,
        0,
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
    )

    fake_formated_response = {
        "uuid": str(agent_uuid),
        "user": "all",
        "version": "0.1.0",
        "hostname": "hostname",
        "ip_address": "1.2.3.4",
        "probing_rate": 1000,
        "buffer_sniffer_size": 10000000,
        "inf_born": 0,
        "sup_born": 32,
        "ips_per_subnet": 6,
        "pfring": False,
        "last_used": datetime.strptime("2020-01-01", "%Y-%m-%d").isoformat(),
    }

    # Test of `.all() method`
    session = FakeSession(response=[(agent_uuid,)])
    assert await DatabaseAgents(session).all() == [
        str(agent_uuid),
    ]

    # Test of `.get() method`
    session = FakeSession(response=[fake_database_response])
    assert await DatabaseAgents(session).get(agent_uuid) == fake_formated_response
    session = FakeSession(response=[])
    assert await DatabaseAgents(session).get(agent_uuid) is None

    parameters = {
        "user": "all",
        "version": "0.1.0",
        "hostname": "hostname",
        "ip_address": "1.2.3.4",
        "probing_rate": 1000,
        "buffer_sniffer_size": 10000000,
        "inf_born": 0,
        "sup_born": 32,
        "ips_per_subnet": 6,
        "pfring": False,
    }

    # Test of `.register() method`
    session = FakeSession(response=None)
    assert await DatabaseAgents(session).register(uuid, parameters) is None

    # Test of `.stamp_last_used() method`
    session = FakeSession(response=None)
    assert await DatabaseAgents(session).stamp_last_used(agent_uuid) is None


@pytest.mark.asyncio
async def test_database_agents_specific(monkeypatch):
    session = FakeSession(response=None)
    assert await DatabaseAgentsSpecific(session).create_table() is None
    assert await DatabaseAgentsSpecific(session).create_table(drop=True) is None

    measurement_uuid_1 = uuid.uuid4()
    agent_uuid_1 = uuid.uuid4()
    fake_database_response_1 = (measurement_uuid_1, agent_uuid_1, 2, 30, 1000, 0)

    measurement_uuid_2 = uuid.uuid4()
    agent_uuid_2 = uuid.uuid4()
    fake_database_response_2 = (measurement_uuid_2, agent_uuid_2, 2, 30, 1000, 1)

    fake_formated_response_1 = {
        "uuid": str(agent_uuid_1),
        "min_ttl": 2,
        "max_ttl": 30,
        "probing_rate": 1000,
        "state": "ongoing",
    }

    fake_formated_response_2 = {
        "uuid": str(agent_uuid_2),
        "min_ttl": 2,
        "max_ttl": 30,
        "probing_rate": 1000,
        "state": "finished",
    }

    # Test of `.all() method`
    session = FakeSession(response=[fake_database_response_1, fake_database_response_2])
    assert await DatabaseAgentsSpecific(session).all(measurement_uuid_1) == [
        fake_formated_response_1,
        fake_formated_response_2,
    ]

    # Test of `.get() method`
    session = FakeSession(response=[fake_database_response_1])
    assert (
        await DatabaseAgentsSpecific(session).get(measurement_uuid_1, agent_uuid_1)
        == fake_formated_response_1
    )
    session = FakeSession(response=[fake_database_response_2])
    assert (
        await DatabaseAgentsSpecific(session).get(measurement_uuid_2, agent_uuid_2)
        == fake_formated_response_2
    )
    session = FakeSession(response=[])
    assert (
        await DatabaseAgentsSpecific(session).get(measurement_uuid_1, agent_uuid_1)
        is None
    )

    parameters = {
        "min_ttl": 2,
        "max_ttl": 30,
        "probing_rate": 1000,
    }

    # Test of `.register() method`
    session = FakeSession(response=[])
    assert (
        await DatabaseAgentsSpecific(session).register(
            measurement_uuid_1, agent_uuid_1, parameters
        )
        is None
    )

    # Test of `.stamp_finished() method`
    session = FakeSession(response=None)
    assert (
        await DatabaseAgentsSpecific(session).stamp_finished(
            measurement_uuid_1, agent_uuid_1
        )
        is None
    )


@pytest.mark.asyncio
async def test_database_measurement_results(monkeypatch):
    measurement_uuid = uuid.UUID("1b830be7-2b42-401b-bbe6-6b1baf02c9be")
    agent_uuid = uuid.UUID("b17fe299-17bf-4dbe-9ae3-f600b540ec1f")

    # Test of `.forge_table_name()` method
    assert DatabaseMeasurementResults.forge_table_name(
        measurement_uuid, agent_uuid
    ) == (
        "results__"
        "1b830be7_2b42_401b_bbe6_6b1baf02c9be__"
        "b17fe299_17bf_4dbe_9ae3_f600b540ec1f"
    )

    # Test of `.parse_table_name()` method
    assert DatabaseMeasurementResults.parse_table_name(
        "results__"
        "1b830be7_2b42_401b_bbe6_6b1baf02c9be__"
        "b17fe299_17bf_4dbe_9ae3_f600b540ec1f"
    ) == {"measurement_uuid": str(measurement_uuid), "agent_uuid": str(agent_uuid)}

    session = FakeSession(response=None)
    assert await DatabaseMeasurementResults(session, "test").create_table() is None
    assert (
        await DatabaseMeasurementResults(session, "test").create_table(drop=True)
        is None
    )

    # Test of `.all_count() method`
    session = FakeSession(response=[(10,)])
    assert await DatabaseMeasurementResults(session, "test").all_count() == 10

    fake_database_response = [
        16909060,
        169090560,
        169090600,
        134744072,
        "udp",
        17000,
        33434,
        5,
        5,
        11,
        0,
        32.4,
        56,
        46,
        1,
    ]

    fake_formated_response_1 = {
        "source_ip": "1.2.3.4",
        "destination_prefix": "10.20.30.0",
        "destination_ip": "10.20.30.40",
        "reply_ip": "8.8.8.8",
        "protocol": "udp",
        "source_port": 17000,
        "destination_port": 33434,
        "ttl": 5,
        "ttl_check": 5,
        "type": 11,
        "code": 0,
        "rtt": 32.4,
        "reply_ttl": 56,
        "reply_size": 46,
        "round": 1,
    }

    # Test of `.all() method`
    session = FakeSession(response=[fake_database_response])
    assert await DatabaseMeasurementResults(session, "test").all(0, 100) == [
        fake_formated_response_1
    ]

    # Test of `.is_exists()` method
    session = FakeSession(response=[(0,)])
    assert await DatabaseMeasurementResults(session, "test").is_exists() is False
    session = FakeSession(response=[(1,)])
    assert await DatabaseMeasurementResults(session, "test").is_exists() is True
