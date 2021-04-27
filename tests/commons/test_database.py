import ipaddress
import json
import uuid
from datetime import datetime

import pytest

from iris.commons.database import (
    Database,
    DatabaseAgents,
    DatabaseAgentsSpecific,
    DatabaseMeasurementResults,
    DatabaseMeasurements,
)
from iris.commons.settings import CommonSettings
from iris.worker.hook import ParametersDataclass


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

    assert await Database(session, CommonSettings()).create_database("test") is None
    assert await Database(session, CommonSettings()).drop_table("test") is None
    assert await Database(session, CommonSettings()).clean_table("test") is None
    assert await Database(session, CommonSettings()).disconnect() is None


@pytest.mark.asyncio
async def test_database_measurements():
    session = FakeSession(response=None)
    assert await DatabaseMeasurements(session, CommonSettings()).create_table() is None
    assert (
        await DatabaseMeasurements(session, CommonSettings()).create_table(drop=True)
        is None
    )

    # Test of `.all_count() method`
    session = FakeSession(response=[(10,)])
    assert (
        await DatabaseMeasurements(session, CommonSettings()).all_count(user="admin")
        == 10
    )

    measurement_uuid_1 = uuid.uuid4()
    fake_database_response_1 = (
        measurement_uuid_1,
        "admin",
        "diamond-miner",
        ["test"],
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
        datetime.strptime("2020-01-02", "%Y-%m-%d"),
    )

    measurement_uuid_2 = uuid.uuid4()
    fake_database_response_2 = (
        measurement_uuid_2,
        "admin",
        "diamond-miner",
        [],
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
        None,
    )

    fake_formated_response_1 = {
        "uuid": str(measurement_uuid_1),
        "user": "admin",
        "tool": "diamond-miner",
        "tags": ["test"],
        "start_time": datetime.strptime("2020-01-01", "%Y-%m-%d").isoformat(),
        "end_time": datetime.strptime("2020-01-02", "%Y-%m-%d").isoformat(),
    }

    fake_formated_response_2 = {
        "uuid": str(measurement_uuid_2),
        "user": "admin",
        "tool": "diamond-miner",
        "tags": [],
        "start_time": datetime.strptime("2020-01-01", "%Y-%m-%d").isoformat(),
        "end_time": None,
    }

    # Test of `.all() method`
    session = FakeSession(response=[fake_database_response_1, fake_database_response_2])
    assert await DatabaseMeasurements(session, CommonSettings()).all(
        "admin", 0, 100
    ) == [
        fake_formated_response_1,
        fake_formated_response_2,
    ]

    # Test of `.get() method`
    session = FakeSession(response=[fake_database_response_1])
    assert (
        await DatabaseMeasurements(session, CommonSettings()).get(
            "admin", measurement_uuid_1
        )
        == fake_formated_response_1
    )
    session = FakeSession(response=[fake_database_response_2])
    assert (
        await DatabaseMeasurements(session, CommonSettings()).get(
            "admin", measurement_uuid_2
        )
        == fake_formated_response_2
    )
    session = FakeSession(response=[])
    assert (
        await DatabaseMeasurements(session, CommonSettings()).get(
            "admin", measurement_uuid_1
        )
        is None
    )

    parameters = {
        "measurement_uuid": measurement_uuid_1,
        "user": "admin",
        "tool": "diamond-miner",
        "tags": ["test"],
        "start_time": 1597829098,
    }

    # Test of `.register() method`
    session = FakeSession(response=None)
    assert (
        await DatabaseMeasurements(session, CommonSettings()).register(parameters)
        is None
    )

    # Test of `.stamp_end_time() method`
    session = FakeSession(response=None)
    assert (
        await DatabaseMeasurements(session, CommonSettings()).stamp_end_time(
            "admin", measurement_uuid_1
        )
        is None
    )


@pytest.mark.asyncio
async def test_database_agents():
    session = FakeSession(response=None)
    assert await DatabaseAgents(session, CommonSettings()).create_table() is None
    assert (
        await DatabaseAgents(session, CommonSettings()).create_table(drop=True) is None
    )

    agent_uuid = uuid.uuid4()
    fake_database_response = (
        agent_uuid,
        "all",
        "0.1.0",
        "hostname",
        "1.2.3.4",
        1,
        1000,
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
    )

    fake_formated_response = {
        "uuid": str(agent_uuid),
        "user": "all",
        "version": "0.1.0",
        "hostname": "hostname",
        "ip_address": "1.2.3.4",
        "min_ttl": 1,
        "max_probing_rate": 1000,
        "last_used": datetime.strptime("2020-01-01", "%Y-%m-%d").isoformat(),
    }

    # Test of `.all() method`
    session = FakeSession(response=[(agent_uuid,)])
    assert await DatabaseAgents(session, CommonSettings()).all() == [
        str(agent_uuid),
    ]

    # Test of `.get() method`
    session = FakeSession(response=[fake_database_response])
    assert (
        await DatabaseAgents(session, CommonSettings()).get(agent_uuid)
        == fake_formated_response
    )
    session = FakeSession(response=[])
    assert await DatabaseAgents(session, CommonSettings()).get(agent_uuid) is None

    parameters = {
        "user": "all",
        "version": "0.1.0",
        "hostname": "hostname",
        "ip_address": "1.2.3.4",
        "min_ttl": 1,
        "max_probing_rate": 1000,
    }

    # Test of `.register() method`
    session = FakeSession(response=None)
    assert (
        await DatabaseAgents(session, CommonSettings()).register(uuid, parameters)
        is None
    )

    # Test of `.stamp_last_used() method`
    session = FakeSession(response=None)
    assert (
        await DatabaseAgents(session, CommonSettings()).stamp_last_used(agent_uuid)
        is None
    )


@pytest.mark.asyncio
async def test_database_agents_specific():
    session = FakeSession(response=None)
    assert (
        await DatabaseAgentsSpecific(session, CommonSettings()).create_table() is None
    )
    assert (
        await DatabaseAgentsSpecific(session, CommonSettings()).create_table(drop=True)
        is None
    )

    measurement_uuid_1 = uuid.uuid4()
    agent_uuid_1 = uuid.uuid4()
    fake_database_response_1 = (
        measurement_uuid_1,
        agent_uuid_1,
        "test.csv",
        1000,
        json.dumps({"parameters": 0}),
        0,
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
    )

    measurement_uuid_2 = uuid.uuid4()
    agent_uuid_2 = uuid.uuid4()
    fake_database_response_2 = (
        measurement_uuid_2,
        agent_uuid_2,
        "test.csv",
        1000,
        json.dumps({"parameters": 0}),
        1,
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
    )

    fake_formated_response_1 = {
        "uuid": str(agent_uuid_1),
        "target_file": "test.csv",
        "probing_rate": 1000,
        "tool_parameters": {"parameters": 0},
        "state": "ongoing",
    }

    fake_formated_response_2 = {
        "uuid": str(agent_uuid_2),
        "target_file": "test.csv",
        "probing_rate": 1000,
        "tool_parameters": {"parameters": 0},
        "state": "finished",
    }

    # Test of `.all() method`
    session = FakeSession(response=[fake_database_response_1, fake_database_response_2])
    assert await DatabaseAgentsSpecific(session, CommonSettings()).all(
        measurement_uuid_1
    ) == [
        fake_formated_response_1,
        fake_formated_response_2,
    ]

    # Test of `.get() method`
    session = FakeSession(response=[fake_database_response_1])
    assert (
        await DatabaseAgentsSpecific(session, CommonSettings()).get(
            measurement_uuid_1, agent_uuid_1
        )
        == fake_formated_response_1
    )
    session = FakeSession(response=[fake_database_response_2])
    assert (
        await DatabaseAgentsSpecific(session, CommonSettings()).get(
            measurement_uuid_2, agent_uuid_2
        )
        == fake_formated_response_2
    )
    session = FakeSession(response=[])
    assert (
        await DatabaseAgentsSpecific(session, CommonSettings()).get(
            measurement_uuid_1, agent_uuid_1
        )
        is None
    )

    agent = ParametersDataclass(
        "agent_uuid",
        {
            "measurement_uuid": "test",
            "target_file": "test.csv",
            "tool_parameters": {
                "min_ttl": 2,
                "max_ttl": 30,
                "max_round": 10,
            },
        },
        {"probing_rate": 100},
        {},
    )

    # Test of `.register() method`
    session = FakeSession(response=[])
    assert (
        await DatabaseAgentsSpecific(session, CommonSettings()).register(agent) is None
    )

    # Test of `.stamp_finished() method`
    session = FakeSession(response=None)
    assert (
        await DatabaseAgentsSpecific(session, CommonSettings()).stamp_finished(
            measurement_uuid_1, agent_uuid_1
        )
        is None
    )


@pytest.mark.asyncio
async def test_database_measurement_results():
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

    # Test of `.swap_table_name_prefix()` method
    assert (
        DatabaseMeasurementResults(
            session, CommonSettings(), "iris.results__measurement__agent"
        ).swap_table_name_prefix("nodes")
        == "iris.nodes__measurement__agent"
    )

    assert (
        await DatabaseMeasurementResults(
            session, CommonSettings(), "test"
        ).create_table()
        is None
    )
    assert (
        await DatabaseMeasurementResults(
            session, CommonSettings(), "test"
        ).create_table(drop=True)
        is None
    )

    # Test of materialized vues creation
    assert (
        await DatabaseMeasurementResults(
            session, CommonSettings(), "iris.results__measurement__agent"
        ).create_materialized_vue_nodes()
        is None
    )

    assert (
        await DatabaseMeasurementResults(
            session, CommonSettings(), "iris.results__measurement__agent"
        ).create_materialized_vue_traceroute()
        is None
    )

    # Test of `.all_count() method`
    session = FakeSession(response=[(10,)])
    assert (
        await DatabaseMeasurementResults(session, CommonSettings(), "test").all_count()
        == 10
    )

    fake_database_response = [
        ipaddress.ip_address("::ffff:ac12:b"),
        ipaddress.ip_address("::ffff:84e3:7b81"),
        24000,
        34334,
        78,
        9,
        ipaddress.ip_address("::ffff:869d:fe0a"),
        17,
        11,
        0,
        37,
        56,
        [1, 2],
        1280.2,
        1,
    ]

    fake_formated_response_1 = {
        "probe_src_addr": "::ffff:ac12:b",
        "probe_dst_addr": "::ffff:84e3:7b81",
        "probe_src_port": 24000,
        "probe_dst_port": 34334,
        "probe_ttl_l3": 78,
        "probe_ttl_l4": 9,
        "reply_src_addr": "::ffff:869d:fe0a",
        "reply_protocol": 17,
        "reply_icmp_type": 11,
        "reply_icmp_code": 0,
        "reply_ttl": 37,
        "reply_size": 56,
        "reply_mpls_labels": [1, 2],
        "rtt": 1280.2,
        "round": 1,
    }

    # Test of `.all() method`
    session = FakeSession(response=[fake_database_response])
    assert await DatabaseMeasurementResults(session, CommonSettings(), "test").all(
        0, 100
    ) == [fake_formated_response_1]

    # Test of `.is_exists()` method
    session = FakeSession(response=[(0,)])
    assert (
        await DatabaseMeasurementResults(session, CommonSettings(), "test").is_exists()
        is False
    )
    session = FakeSession(response=[(1,)])
    assert (
        await DatabaseMeasurementResults(session, CommonSettings(), "test").is_exists()
        is True
    )
