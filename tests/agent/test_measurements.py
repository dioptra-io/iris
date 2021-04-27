import pytest

from iris.agent.measurements import build_probe_generator_parameters
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
        "probing_rate": 100,
        "targets_file": "prefixes.txt",
        "tool": "diamond-miner",
        "tool_parameters": {
            "protocol": "icmp",
            "initial_source_port": 24000,
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 32,
            "max_round": 10,
            "n_flow_ids": 6,
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


def test_build_probe_generator_parameters():

    targets_file = ["8.8.8.0/24", "8.8.4.0/24"]
    parameters = ParametersDataclass.from_request(request)
    prober_parameters = build_probe_generator_parameters(targets_file, parameters)

    assert prober_parameters["prefixes"] == [
        ("8.8.8.0/24", "icmp", range(2, 33)),
        ("8.8.4.0/24", "icmp", range(2, 33)),
    ]
    assert prober_parameters["prefix_len_v4"] == 24
    assert prober_parameters["prefix_len_v6"] == 64
    assert prober_parameters["flow_ids"] == range(6)
    assert prober_parameters["probe_dst_port"] == 33434

    request["parameters"]["tool"] = "yarrp"
    request["parameters"]["tool_parameters"]["n_flow_ids"] = 1
    parameters = ParametersDataclass.from_request(request)
    prober_parameters = build_probe_generator_parameters(targets_file, parameters)

    assert prober_parameters["prefixes"] == [
        ("8.8.8.0/24", "icmp", range(2, 33)),
        ("8.8.4.0/24", "icmp", range(2, 33)),
    ]
    assert prober_parameters["prefix_len_v4"] == 24
    assert prober_parameters["prefix_len_v6"] == 64
    assert prober_parameters["flow_ids"] == range(1)
    assert prober_parameters["probe_dst_port"] == 33434

    targets_file = ["8.8.8.8", "8.8.4.4"]
    request["parameters"]["tool"] = "ping"
    request["parameters"]["tool_parameters"]["n_flow_ids"] = 1
    parameters = ParametersDataclass.from_request(request)
    prober_parameters = build_probe_generator_parameters(targets_file, parameters)

    assert prober_parameters["prefixes"] == [
        ("8.8.8.8", "icmp", [32]),
        ("8.8.4.4", "icmp", [32]),
    ]
    assert prober_parameters["prefix_len_v4"] == 32
    assert prober_parameters["prefix_len_v6"] == 128
    assert prober_parameters["flow_ids"] == range(1)
    assert prober_parameters["probe_dst_port"] == 33434

    request["parameters"]["tool"] = "test"
    parameters = ParametersDataclass.from_request(request)
    with pytest.raises(ValueError):
        prober_parameters = build_probe_generator_parameters(targets_file, parameters)
