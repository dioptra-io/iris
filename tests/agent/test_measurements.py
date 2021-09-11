from iris.agent.measurements import build_probe_generator_parameters
from iris.commons.round import Round
from iris.commons.schemas import public


def unordered_eq(a, b):
    assert len(a) == len(b)
    assert set(a) == set(b)


def test_build_probe_generator_parameters_diamond_miner():
    tool = public.Tool.DiamondMiner
    tool_parameters = public.ToolParameters(
        initial_source_port=24000,
        destination_port=33434,
        max_round=10,
        n_flow_ids=6,
        flow_mapper=public.FlowMapper.SequentialFlowMapper,
        flow_mapper_kwargs=None,
    )

    # D-Miner: Base case
    target_list = ["8.8.8.0/24,icmp,2,32", "8.8.4.0/24,icmp,2,32"]
    p = build_probe_generator_parameters(
        2, Round(1, 0, 0), tool, tool_parameters, target_list, None
    )
    unordered_eq(
        p["prefixes"],
        [("8.8.4.0/24", "icmp", range(2, 33)), ("8.8.8.0/24", "icmp", range(2, 33))],
    )
    assert p["prefix_len_v4"] == 24
    assert p["prefix_len_v6"] == 64
    assert p["flow_ids"] == range(6)
    assert p["probe_dst_port"] == 33434

    # D-Miner: Different agent min ttl
    target_list = ["8.8.8.0/24,icmp,2,32", "8.8.4.0/24,icmp,2,32"]
    p = build_probe_generator_parameters(
        6, Round(1, 0, 0), tool, tool_parameters, target_list, None
    )
    unordered_eq(
        p["prefixes"],
        [("8.8.4.0/24", "icmp", range(6, 33)), ("8.8.8.0/24", "icmp", range(6, 33))],
    )

    # D-Miner: Same prefix twice
    target_list = ["8.8.8.0/24,icmp,2,32", "8.8.8.0/24,icmp,2,20"]
    p = build_probe_generator_parameters(
        6, Round(1, 0, 0), tool, tool_parameters, target_list, None
    )
    unordered_eq(
        p["prefixes"],
        [("8.8.8.0/24", "icmp", range(6, 33)), ("8.8.8.0/24", "icmp", range(6, 21))],
    )

    # D-Miner: for subround 1.1, 1.2, ...
    target_list = ["8.8.4.0/24,icmp,2,32", "8.8.8.0/24,icmp,2,20"]
    prefix_list = ["8.8.4.0/24", "8.8.8.0/24"]
    p = build_probe_generator_parameters(
        6, Round(1, 0, 0), tool, tool_parameters, target_list, prefix_list
    )
    unordered_eq(
        p["prefixes"],
        [("8.8.4.0/24", "icmp", range(6, 33)), ("8.8.8.0/24", "icmp", range(6, 21))],
    )

    target_list = ["8.8.4.0/24,icmp,2,32", "8.8.8.0/24,icmp,2,20"]
    prefix_list = ["8.8.4.0/24"]
    p = build_probe_generator_parameters(
        6, Round(1, 0, 0), tool, tool_parameters, target_list, prefix_list
    )
    unordered_eq(p["prefixes"], [("8.8.4.0/24", "icmp", range(6, 33))])

    target_list = ["8.8.4.0/22,icmp,2,32"]
    prefix_list = ["8.8.4.0/24"]
    p = build_probe_generator_parameters(
        6, Round(1, 0, 0), tool, tool_parameters, target_list, prefix_list
    )
    unordered_eq(p["prefixes"], [("8.8.4.0/24", "icmp", range(6, 33))])


def test_build_probe_generator_parameters_yarrp():
    tool = public.Tool.Yarrp
    tool_parameters = public.ToolParameters(
        initial_source_port=24000,
        destination_port=33434,
        max_round=10,
        n_flow_ids=1,
        flow_mapper=public.FlowMapper.SequentialFlowMapper,
        flow_mapper_kwargs=None,
    )

    target_list = ["8.8.8.0/24,icmp,2,32", "8.8.4.0/24,icmp,2,32"]
    p = build_probe_generator_parameters(
        2, Round(1, 0, 0), tool, tool_parameters, target_list, None
    )

    unordered_eq(
        p["prefixes"],
        [("8.8.4.0/24", "icmp", range(2, 33)), ("8.8.8.0/24", "icmp", range(2, 33))],
    )
    assert p["prefix_len_v4"] == 24
    assert p["prefix_len_v6"] == 64
    assert p["flow_ids"] == range(1)
    assert p["probe_dst_port"] == 33434


def test_build_probe_generator_parameters_ping():
    tool = public.Tool.Ping
    tool_parameters = public.ToolParameters(
        initial_source_port=24000,
        destination_port=33434,
        max_round=10,
        n_flow_ids=1,
        flow_mapper=public.FlowMapper.SequentialFlowMapper,
        flow_mapper_kwargs=None,
        prefix_len_v4=32,
        prefix_len_v6=128,
    )

    target_list = ["8.8.8.8,icmp,2,32", "8.8.4.4,icmp,2,32"]
    p = build_probe_generator_parameters(
        2, Round(1, 0, 0), tool, tool_parameters, target_list, None
    )

    unordered_eq(
        p["prefixes"],
        [("8.8.4.4", "icmp", (32,)), ("8.8.8.8", "icmp", (32,))],
    )
    assert p["prefix_len_v4"] == 32
    assert p["prefix_len_v6"] == 128
    assert p["flow_ids"] == range(1)
    assert p["probe_dst_port"] == 33434
