import pytest

from iris.agent.measurements import (
    build_probe_generator_parameters,
    build_prober_parameters,
)

request = {
    "measurement_uuid": "ab59dc2d-95d0-4af5-aef6-b75e1a96a13f",
    "measurement_tool": "diamond-miner",
    "username": "admin",
    "round": 1,
    "probes": None,
    "parameters": {
        "version": "0.6.1",
        "hostname": "b581c1224f87",
        "ip_address": "172.22.0.12",
        "probing_rate": 100,
        "targets_file": "prefixes.txt",
        "tool": "diamond-miner",
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


def test_build_prober_parameters():
    local_request = request.copy()
    parameters = build_prober_parameters(local_request)
    assert parameters["user"] == "admin"
    assert parameters["version"] == "0.6.1"
    assert parameters["tool"] == "diamond-miner"
    assert parameters["tool_parameters"]["protocol"] == "udp"


def test_build_probe_generator_parameters():
    local_request = request.copy()
    parameters = build_prober_parameters(local_request)
    prober_parameters = build_probe_generator_parameters(parameters)

    assert prober_parameters["prefix_len_v4"] == 24
    assert prober_parameters["prefix_len_v6"] == 64
    assert prober_parameters["flow_ids"] == range(0, 6)
    assert prober_parameters["ttls"] == range(2, 33)
    assert prober_parameters["probe_dst_port"] == 33434

    parameters["tool"] = "diamond-miner-ping"
    prober_parameters = build_probe_generator_parameters(parameters)

    assert prober_parameters["prefix_len_v4"] == 32
    assert prober_parameters["prefix_len_v6"] == 128
    assert prober_parameters["flow_ids"] == [0]
    assert prober_parameters["ttls"] == [32]
    assert prober_parameters["probe_dst_port"] == 33434

    parameters["tool"] = "tests"
    with pytest.raises(ValueError):
        prober_parameters = build_probe_generator_parameters(parameters)
