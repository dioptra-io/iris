import uuid

import pytest

from iris.commons.dataclasses import ParametersDataclass

request = {
    "measurement_uuid": "ab59dc2d-95d0-4af5-aef6-b75e1a96a13f",
    "username": "admin",
    "round": 1,
    "probes": None,
    "parameters": {
        "version": "0.6.1",
        "hostname": "b581c1224f87",
        "ip_address": "172.22.0.12",
        "min_ttl": 1,
        "max_probing_rate": 100,
        "targets_file": "prefixes.csv",
        "tool": "diamond-miner",
        "probing_rate": None,
        "tool_parameters": {
            "protocol": "udp",
            "initial_source_port": 24000,
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 32,
            "max_round": 10,
            "flow_mapper": "IntervalFlowMapper",
            "flow_mapper_kwargs": None,
        },
        "tags": ["test"],
        "measurement_uuid": "ab59dc2d-95d0-4af5-aef6-b75e1a96a13f",
        "user": "admin",
        "start_time": 1617270732.905208,
        "agent_uuid": "6a3af939-e23e-4470-aa40-a04a1fb1b21c",
    },
}


def test_parameters_dataclass():
    measurement_uuid = str(uuid.uuid4())
    agent_uuid = str(uuid.uuid4())

    agent = ParametersDataclass(
        agent_uuid,
        {
            "tool": "diamond-miner",
            "measurement_uuid": measurement_uuid,
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
            "targets_file": "custom.csv",
            "probing_rate": 200,
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
        },
    )

    with pytest.raises(AttributeError):
        assert agent.test

    assert agent.dict() == {
        "agent_uuid": agent_uuid,
        "targets_file": "custom.csv",
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
        "version": "0.1.0",
        "hostname": "hostname",
        "ip_address": "1.2.3.4",
        "min_ttl": 1,
        "max_probing_rate": 1000,
        "probing_rate": 200,
        "tags": ["test"],
    }

    parameters = ParametersDataclass.from_request(request)
    assert parameters.dict() == {
        "user": "admin",
        "version": "0.6.1",
        "hostname": "b581c1224f87",
        "ip_address": "172.22.0.12",
        "min_ttl": 1,
        "max_probing_rate": 100,
        "targets_file": "prefixes.csv",
        "tool": "diamond-miner",
        "probing_rate": None,
        "tool_parameters": {
            "protocol": "udp",
            "initial_source_port": 24000,
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 32,
            "max_round": 10,
            "flow_mapper": "IntervalFlowMapper",
            "flow_mapper_kwargs": None,
        },
        "measurement_uuid": "ab59dc2d-95d0-4af5-aef6-b75e1a96a13f",
        "start_time": 1617270732.905208,
        "agent_uuid": "6a3af939-e23e-4470-aa40-a04a1fb1b21c",
        "tags": ["test"],
    }
