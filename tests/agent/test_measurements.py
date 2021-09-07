from unittest import mock

import aiofiles
import pytest

from iris.agent.measurements import build_probe_generator_parameters
from iris.agent.settings import AgentSettings
from iris.commons.dataclasses import ParametersDataclass
from iris.commons.round import Round

settings = AgentSettings()

request = {
    "measurement_uuid": "ab59dc2d-95d0-4af5-aef6-b75e1a96a13f",
    "username": "admin",
    "round": 1,
    "probes": None,
    "parameters": {
        "version": "0.6.1",
        "hostname": "b581c1224f87",
        "ipv4_address": "1.2.3.4",
        "ipv6_address": "::1234",
        "min_ttl": 1,
        "max_probing_rate": 100,
        "agent_tags": ["all"],
        "probing_rate": 100,
        "target_file": "prefixes.csv",
        "tool": "diamond-miner",
        "tool_parameters": {
            "initial_source_port": 24000,
            "destination_port": 33434,
            "max_round": 10,
            "n_flow_ids": 6,
            "flow_mapper": "SequentialFlowMapper",
            "flow_mapper_kwargs": None,
        },
        "tags": ["test"],
        "measurement_uuid": "ab59dc2d-95d0-4af5-aef6-b75e1a96a13f",
        "user": "admin",
        "start_time": 1617270732.905208,
        "agent_uuid": "6a3af939-e23e-4470-aa40-a04a1fb1b21c",
    },
}


aiofiles.threadpool.wrap.register(mock.MagicMock)(
    lambda *args, **kwargs: aiofiles.threadpool.AsyncBufferedIOBase(*args, **kwargs)
)


def unordered_eq(a, b):
    assert len(a) == len(b)
    assert set(a) == set(b)


def test_build_probe_generator_parameters(tmp_path):
    # D-Miner
    settings.AGENT_MIN_TTL = 2
    parameters = ParametersDataclass.from_request(request)

    target_filepath = tmp_path / "targets"
    prefix_filepath = tmp_path / "prefixes"

    target_filepath.write_text("8.8.8.0/24,icmp,2,32\n8.8.4.0/24,icmp,2,32")
    prober_parameters = build_probe_generator_parameters(
        settings, target_filepath, None, Round(1, 0, 0), parameters
    )

    unordered_eq(
        prober_parameters["prefixes"],
        [("8.8.4.0/24", "icmp", range(2, 33)), ("8.8.8.0/24", "icmp", range(2, 33))],
    )
    assert prober_parameters["prefix_len_v4"] == 24
    assert prober_parameters["prefix_len_v6"] == 64
    assert prober_parameters["flow_ids"] == range(6)
    assert prober_parameters["probe_dst_port"] == 33434

    # D-Miner: Different agent min ttl
    settings.AGENT_MIN_TTL = 6
    parameters = ParametersDataclass.from_request(request)

    target_filepath.write_text("8.8.8.0/24,icmp,2,32\n8.8.4.0/24,icmp,2,32")
    prober_parameters = build_probe_generator_parameters(
        settings, target_filepath, None, Round(1, 0, 0), parameters
    )

    unordered_eq(
        prober_parameters["prefixes"],
        [("8.8.4.0/24", "icmp", range(6, 33)), ("8.8.8.0/24", "icmp", range(6, 33))],
    )

    # D-Miner: Same prefix twice
    settings.AGENT_MIN_TTL = 6
    parameters = ParametersDataclass.from_request(request)

    target_filepath.write_text("8.8.8.0/24,icmp,2,32\n8.8.8.0/24,icmp,2,20")
    prober_parameters = build_probe_generator_parameters(
        settings, target_filepath, None, Round(1, 0, 0), parameters
    )

    unordered_eq(
        prober_parameters["prefixes"],
        [("8.8.8.0/24", "icmp", range(6, 33)), ("8.8.8.0/24", "icmp", range(6, 21))],
    )

    # D-Miner: for subround 1.1, 1.2, ...
    settings.AGENT_MIN_TTL = 6
    parameters = ParametersDataclass.from_request(request)

    target_filepath.write_text("8.8.4.0/24,icmp,2,32\n8.8.8.0/24,icmp,2,20")
    prefix_filepath.write_text("8.8.4.0/24\n8.8.8.0/24")
    prober_parameters = build_probe_generator_parameters(
        settings, target_filepath, prefix_filepath, Round(1, 0, 0), parameters
    )

    unordered_eq(
        prober_parameters["prefixes"],
        [("8.8.4.0/24", "icmp", range(6, 33)), ("8.8.8.0/24", "icmp", range(6, 21))],
    )

    settings.AGENT_MIN_TTL = 6
    parameters = ParametersDataclass.from_request(request)

    target_filepath.write_text("8.8.4.0/24,icmp,2,32\n8.8.8.0/24,icmp,2,20")
    prefix_filepath.write_text("8.8.4.0/24\n")
    prober_parameters = build_probe_generator_parameters(
        settings, target_filepath, prefix_filepath, Round(1, 0, 0), parameters
    )

    unordered_eq(prober_parameters["prefixes"], [("8.8.4.0/24", "icmp", range(6, 33))])

    settings.AGENT_MIN_TTL = 6
    parameters = ParametersDataclass.from_request(request)

    target_filepath.write_text("8.8.4.0/22,icmp,2,32")
    prefix_filepath.write_text("8.8.4.0/24\n")
    prober_parameters = build_probe_generator_parameters(
        settings, target_filepath, prefix_filepath, Round(1, 0, 0), parameters
    )

    unordered_eq(prober_parameters["prefixes"], [("8.8.4.0/24", "icmp", range(6, 33))])

    # YARRP
    settings.AGENT_MIN_TTL = 2
    request["parameters"]["tool"] = "yarrp"
    request["parameters"]["tool_parameters"]["n_flow_ids"] = 1
    parameters = ParametersDataclass.from_request(request)
    target_filepath.write_text("8.8.8.0/24,icmp,2,32\n8.8.4.0/24,icmp,2,32")
    prober_parameters = build_probe_generator_parameters(
        settings, target_filepath, None, Round(1, 0, 0), parameters
    )

    unordered_eq(
        prober_parameters["prefixes"],
        [("8.8.4.0/24", "icmp", range(2, 33)), ("8.8.8.0/24", "icmp", range(2, 33))],
    )
    assert prober_parameters["prefix_len_v4"] == 24
    assert prober_parameters["prefix_len_v6"] == 64
    assert prober_parameters["flow_ids"] == range(1)
    assert prober_parameters["probe_dst_port"] == 33434

    # Ping
    request["parameters"]["tool"] = "ping"
    request["parameters"]["tool_parameters"]["n_flow_ids"] = 1
    parameters = ParametersDataclass.from_request(request)
    target_filepath.write_text("8.8.8.8,icmp,2,32\n8.8.4.4,icmp,2,32")
    prober_parameters = build_probe_generator_parameters(
        settings, target_filepath, None, Round(1, 0, 0), parameters
    )

    unordered_eq(
        prober_parameters["prefixes"],
        [("8.8.4.4", "icmp", (32,)), ("8.8.8.8", "icmp", (32,))],
    )
    assert prober_parameters["prefix_len_v4"] == 32
    assert prober_parameters["prefix_len_v6"] == 128
    assert prober_parameters["flow_ids"] == range(1)
    assert prober_parameters["probe_dst_port"] == 33434

    # Invalid tool
    request["parameters"]["tool"] = "test"
    parameters = ParametersDataclass.from_request(request)
    target_filepath.write_text("8.8.8.0/24,icmp,2,32\n8.8.4.0/24,icmp,2,32")
    with pytest.raises(AssertionError):
        prober_parameters = build_probe_generator_parameters(
            settings, target_filepath, None, Round(1, 0, 0), parameters
        )
