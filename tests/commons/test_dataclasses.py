"""Test of commons dataclasses."""

import uuid

import pytest

from iris.commons.dataclasses import ParametersDataclass


def test_parameters_dataclass():
    """Test of `ParametersDataclass` class."""

    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    agent = ParametersDataclass(
        agent_uuid,
        {
            "targets_file": "prefixes.txt",
            "tool": "diamond-miner",
            "tool_parameters": {
                "protocol": "udp",
                "initial_source_port": 24000,
                "destination_port": 33434,
                "min_ttl": 5,
                "max_ttl": 20,
                "max_round": 10,
                "flow_mapper": "IntervalFlowMapper",
                "flow_mapper_kwargs": None,
            },
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
            "targets_file": "custom.txt",
            "probing_rate": 200,
            "tool_parameters": {"min_ttl": 10},
        },
    )

    with pytest.raises(AttributeError):
        assert agent.test

    assert agent.dict() == {
        "agent_uuid": agent_uuid,
        "targets_file": "custom.txt",
        "tool": "diamond-miner",
        "tool_parameters": {
            "protocol": "udp",
            "initial_source_port": 24000,
            "destination_port": 33434,
            "min_ttl": 10,
            "max_ttl": 20,
            "max_round": 10,
            "flow_mapper": "IntervalFlowMapper",
            "flow_mapper_kwargs": None,
        },
        "measurement_uuid": measurement_uuid,
        "user": "admin",
        "start_time": 1605630993.092607,
        "version": "0.1.0",
        "hostname": "hostname",
        "ip_address": "1.2.3.4",
        "probing_rate": 200,
    }
