"""Test of commons dataclasses."""

import random
import uuid

import pytest

from iris.commons.dataclasses import ParametersDataclass


def test_parameters_dataclass():
    """Test of `ParametersDataclass` class."""

    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    random.seed(27)
    agent = ParametersDataclass(
        agent_uuid,
        {
            "targets_file_key": "test.txt",
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 5,
            "max_ttl": 30,
            "max_round": 10,
            "measurement_uuid": measurement_uuid,
            "user": "admin",
            "start_time": 1605630993.092607,
        },
        {
            "user": "all",
            "version": "0.1.0",
            "hostname": "hostname",
            "ip_address": "1.2.3.4",
            "probing_rate": 1000,
        },
        {
            "targets_file_key": None,
            "min_ttl": None,
            "max_ttl": 20,
            "probing_rate": None,
            "max_round": None,
        },
    )

    assert agent.agent_uuid == agent_uuid
    assert agent.measurement_uuid == measurement_uuid
    assert agent.user == "admin"
    assert agent.probing_rate == 1000
    assert agent.targets_file_key == "test.txt"
    assert agent.min_ttl == 5
    assert agent.max_ttl == 20
    assert agent.seed == 2785274337

    with pytest.raises(AttributeError):
        assert agent.test

    assert agent.to_dict() == {
        "agent_uuid": agent_uuid,
        "targets_file_key": "test.txt",
        "protocol": "udp",
        "destination_port": 33434,
        "min_ttl": 5,
        "max_ttl": 20,
        "max_round": 10,
        "seed": 2785274337,
        "measurement_uuid": measurement_uuid,
        "user": "admin",
        "start_time": 1605630993.092607,
        "version": "0.1.0",
        "hostname": "hostname",
        "ip_address": "1.2.3.4",
        "probing_rate": 1000,
    }
