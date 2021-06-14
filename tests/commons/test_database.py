import ipaddress
import json
import uuid
from datetime import datetime

import pytest

from iris.commons.database import (
    Database,
    DatabaseAgents,
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
        "canceled",
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
        datetime.strptime("2020-01-02", "%Y-%m-%d"),
    )

    measurement_uuid_2 = uuid.uuid4()
    fake_database_response_2 = (
        measurement_uuid_2,
        "admin",
        "diamond-miner",
        [],
        "ongoing",
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
        None,
    )

    fake_formated_response_1 = {
        "uuid": str(measurement_uuid_1),
        "user": "admin",
        "tool": "diamond-miner",
        "tags": ["test"],
        "state": "canceled",
        "start_time": datetime.strptime("2020-01-01", "%Y-%m-%d").isoformat(),
        "end_time": datetime.strptime("2020-01-02", "%Y-%m-%d").isoformat(),
    }

    fake_formated_response_2 = {
        "uuid": str(measurement_uuid_2),
        "user": "admin",
        "tool": "diamond-miner",
        "tags": [],
        "state": "ongoing",
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

    measurement_uuid_1 = uuid.uuid4()
    agent_uuid_1 = uuid.uuid4()
    fake_database_response_1 = (
        measurement_uuid_1,
        agent_uuid_1,
        "test.csv",
        1000,
        json.dumps({"agent": 0}),
        json.dumps({"parameters": 0}),
        "ongoing",
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
    )

    measurement_uuid_2 = uuid.uuid4()
    agent_uuid_2 = uuid.uuid4()
    fake_database_response_2 = (
        measurement_uuid_2,
        agent_uuid_2,
        "test.csv",
        1000,
        json.dumps({"agent": 0}),
        json.dumps({"parameters": 0}),
        "finished",
        datetime.strptime("2020-01-01", "%Y-%m-%d"),
    )

    fake_formated_response_1 = {
        "uuid": str(agent_uuid_1),
        "target_file": "test.csv",
        "probing_rate": 1000,
        "tool_parameters": {"parameters": 0},
        "agent_parameters": {"agent": 0},
        "state": "ongoing",
    }

    fake_formated_response_2 = {
        "uuid": str(agent_uuid_2),
        "target_file": "test.csv",
        "probing_rate": 1000,
        "tool_parameters": {"parameters": 0},
        "agent_parameters": {"agent": 0},
        "state": "finished",
    }

    # Test of `.all() method`
    session = FakeSession(response=[fake_database_response_1, fake_database_response_2])
    assert await DatabaseAgents(session, CommonSettings()).all(measurement_uuid_1) == [
        fake_formated_response_1,
        fake_formated_response_2,
    ]

    # Test of `.get() method`
    session = FakeSession(response=[fake_database_response_1])
    assert (
        await DatabaseAgents(session, CommonSettings()).get(
            measurement_uuid_1, agent_uuid_1
        )
        == fake_formated_response_1
    )
    session = FakeSession(response=[fake_database_response_2])
    assert (
        await DatabaseAgents(session, CommonSettings()).get(
            measurement_uuid_2, agent_uuid_2
        )
        == fake_formated_response_2
    )
    session = FakeSession(response=[])
    assert (
        await DatabaseAgents(session, CommonSettings()).get(
            measurement_uuid_1, agent_uuid_1
        )
        is None
    )

    agent = ParametersDataclass(
        agent_uuid_1,
        {
            "tool": "diamond-miner",
            "measurement_uuid": measurement_uuid_1,
            "user": "admin",
            "tags": ["test"],
            "start_time": 1605630993.092607,
        },
        {
            "user": "all",
            "version": "0.1.0",
            "hostname": "hostname",
            "ip_address": "1.2.3.4",
            "min_ttl": 1,
            "max_probing_rate": 1000,
        },
        {
            "target_file": "custom.csv",
            "probing_rate": 200,
            "tool_parameters": {
                "protocol": "udp",
                "initial_source_port": 24000,
                "destination_port": 33434,
                "global_min_ttl": 5,
                "global_max_ttl": 20,
                "max_round": 10,
                "flow_mapper": "IntervalFlowMapper",
                "flow_mapper_kwargs": None,
            },
        },
    )

    # Test of `.register() method`
    session = FakeSession(response=[])
    assert await DatabaseAgents(session, CommonSettings()).register(agent) is None

    # Test of `.stamp_finished() method`
    session = FakeSession(response=None)
    assert (
        await DatabaseAgents(session, CommonSettings()).stamp_finished(
            measurement_uuid_1, agent_uuid_1
        )
        is None
    )


@pytest.mark.asyncio
async def test_database_measurement_results():
    session = FakeSession(response=None)

    # assert (
    #     await DatabaseMeasurementResults(
    #         session,
    #         CommonSettings(),
    #         "measurement",
    #         "agent",
    #     ).create_table()
    #     is None
    # )

    # Test of `.all_count() method`
    session = FakeSession(response=[(10,)])
    assert (
        await DatabaseMeasurementResults(
            session, CommonSettings(), "measurement", "agent"
        ).all_count()
        == 10
    )

    fake_database_response = [
        0,
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
        "reply_mpls_labels": [1, 2],
        "rtt": 1280.2,
        "round": 1,
    }

    # Test of `.all() method`
    session = FakeSession(response=[fake_database_response])
    assert await DatabaseMeasurementResults(
        session, CommonSettings(), "measurement", "agent"
    ).all(0, 100) == [fake_formated_response_1]

    # Test of `.is_exists()` method
    session = FakeSession(response=[(0,)])
    assert (
        await DatabaseMeasurementResults(
            session, CommonSettings(), "measurement", "agent"
        ).is_exists()
        is False
    )
    session = FakeSession(response=[(1,)])
    assert (
        await DatabaseMeasurementResults(
            session, CommonSettings(), "measurement", "agent"
        ).is_exists()
        is True
    )
