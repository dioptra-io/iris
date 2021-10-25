import uuid

from iris.agent.probe_generator import build_probe_generator_parameters
from iris.commons.schemas import public
from iris.commons.schemas.public import Round


def unordered_eq(a, b):
    assert len(a) == len(b)
    assert set(a) == set(b)


def test_build_probe_generator_parameters_diamond_miner():
    measurement = public.MeasurementPostBody(
        tool=public.Tool.DiamondMiner,
        agents=[
            public.MeasurementAgentPostBody(
                uuid=uuid.uuid4(),
                tag=None,
                target_file="test.csv",
                tool_parameters=public.ToolParameters(
                    initial_source_port=24000,
                    destination_port=33434,
                    max_round=10,
                    failure_rate=0.05,
                    flow_mapper=public.FlowMapper.SequentialFlowMapper,
                    flow_mapper_kwargs=None,
                ),
            )
        ],
        tags=["test"],
    )

    # D-Miner: Base case
    target_list = ["8.8.8.0/24,icmp,2,32", "8.8.4.0/24,icmp,2,32"]
    p = build_probe_generator_parameters(
        2,
        Round(number=1, limit=0, offset=0),
        measurement.tool,
        measurement.agents[0].tool_parameters,
        target_list,
        None,
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
        6,
        Round(number=1, limit=0, offset=0),
        measurement.tool,
        measurement.agents[0].tool_parameters,
        target_list,
        None,
    )
    unordered_eq(
        p["prefixes"],
        [("8.8.4.0/24", "icmp", range(6, 33)), ("8.8.8.0/24", "icmp", range(6, 33))],
    )

    # D-Miner: Same prefix twice
    target_list = ["8.8.8.0/24,icmp,2,32", "8.8.8.0/24,icmp,2,20"]
    p = build_probe_generator_parameters(
        6,
        Round(number=1, limit=0, offset=0),
        measurement.tool,
        measurement.agents[0].tool_parameters,
        target_list,
        None,
    )
    unordered_eq(
        p["prefixes"],
        [("8.8.8.0/24", "icmp", range(6, 33)), ("8.8.8.0/24", "icmp", range(6, 21))],
    )

    # D-Miner: for subround 1.1, 1.2, ...
    target_list = ["8.8.4.0/24,icmp,2,32", "8.8.8.0/24,icmp,2,20"]
    prefix_list = ["8.8.4.0/24", "8.8.8.0/24"]
    p = build_probe_generator_parameters(
        6,
        Round(number=1, limit=0, offset=0),
        measurement.tool,
        measurement.agents[0].tool_parameters,
        target_list,
        prefix_list,
    )
    unordered_eq(
        p["prefixes"],
        [("8.8.4.0/24", "icmp", range(6, 33)), ("8.8.8.0/24", "icmp", range(6, 21))],
    )

    target_list = ["8.8.4.0/24,icmp,2,32", "8.8.8.0/24,icmp,2,20"]
    prefix_list = ["8.8.4.0/24"]
    p = build_probe_generator_parameters(
        6,
        Round(number=1, limit=0, offset=0),
        measurement.tool,
        measurement.agents[0].tool_parameters,
        target_list,
        prefix_list,
    )
    unordered_eq(p["prefixes"], [("8.8.4.0/24", "icmp", range(6, 33))])

    target_list = ["8.8.4.0/22,icmp,2,32"]
    prefix_list = ["8.8.4.0/24"]
    p = build_probe_generator_parameters(
        6,
        Round(number=1, limit=0, offset=0),
        measurement.tool,
        measurement.agents[0].tool_parameters,
        target_list,
        prefix_list,
    )
    unordered_eq(p["prefixes"], [("8.8.4.0/24", "icmp", range(6, 33))])


def test_build_probe_generator_parameters_yarrp():
    measurement = public.MeasurementPostBody(
        tool=public.Tool.Yarrp,
        agents=[
            public.MeasurementAgentPostBody(
                uuid=uuid.uuid4(),
                tag=None,
                target_file="test.csv",
                tool_parameters=public.ToolParameters(
                    initial_source_port=24000,
                    destination_port=33434,
                    max_round=10,
                    flow_mapper=public.FlowMapper.SequentialFlowMapper,
                    flow_mapper_kwargs=None,
                ),
            )
        ],
        tags=["test"],
    )

    target_list = ["8.8.8.0/24,icmp,2,32", "8.8.4.0/24,icmp,2,32"]
    p = build_probe_generator_parameters(
        2,
        Round(number=1, limit=0, offset=0),
        measurement.tool,
        measurement.agents[0].tool_parameters,
        target_list,
        None,
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
    measurement = public.MeasurementPostBody(
        tool=public.Tool.Ping,
        agents=[
            public.MeasurementAgentPostBody(
                uuid=uuid.uuid4(),
                tag=None,
                target_file="test.csv",
                tool_parameters=public.ToolParameters(
                    initial_source_port=24000,
                    destination_port=33434,
                    max_round=10,
                    flow_mapper=public.FlowMapper.SequentialFlowMapper,
                    flow_mapper_kwargs=None,
                    prefix_len_v4=32,
                    prefix_len_v6=128,
                ),
            )
        ],
        tags=["test"],
    )

    target_list = ["8.8.8.8,icmp,2,32", "8.8.4.4,icmp,2,32"]
    p = build_probe_generator_parameters(
        2,
        Round(number=1, limit=0, offset=0),
        measurement.tool,
        measurement.agents[0].tool_parameters,
        target_list,
        None,
    )

    unordered_eq(
        p["prefixes"],
        [("8.8.4.4", "icmp", (32,)), ("8.8.8.8", "icmp", (32,))],
    )
    assert p["prefix_len_v4"] == 32
    assert p["prefix_len_v6"] == 128
    assert p["flow_ids"] == range(1)
    assert p["probe_dst_port"] == 33434
