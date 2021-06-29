import logging
import uuid

import pytest

from iris.commons.database import Agents
from iris.commons.dataclasses import ParametersDataclass


@pytest.mark.asyncio
async def test_agents(common_settings):
    db = Agents(common_settings, logging.getLogger(__name__))
    assert await db.create_database() is None
    assert await db.create_table(drop=True) is None

    agent_uuid = uuid.uuid4()
    measurement_uuid = uuid.uuid4()

    obj = ParametersDataclass(
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
            "ipv4_address": "1.2.3.4",
            "ipv6_address": "::1234",
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

    formatted = {
        "uuid": str(obj.agent_uuid),
        "target_file": obj.target_file,
        "probing_rate": obj.probing_rate,
        "probing_statistics": {},
        "agent_parameters": obj.agent_parameters,
        "tool_parameters": obj.tool_parameters,
        "state": "ongoing",
    }

    assert await db.register(obj) is None
    assert await db.get(uuid.uuid4(), uuid.uuid4()) is None
    assert await db.all(measurement_uuid=uuid.uuid4()) == []
    assert await db.all(measurement_uuid=measurement_uuid) == [formatted]
    assert (
        await db.get(measurement_uuid=measurement_uuid, agent_uuid=agent_uuid)
        == formatted
    )

    assert (
        await db.store_probing_statistics(
            measurement_uuid, agent_uuid, "1:0:0", {"packets_sent": 10}
        )
        is None
    )

    assert (await db.get(measurement_uuid=measurement_uuid, agent_uuid=agent_uuid))[
        "probing_statistics"
    ] == {"1:0:0": {"packets_sent": 10}}

    assert (
        await db.store_probing_statistics(
            measurement_uuid, agent_uuid, "2:0:0", {"packets_sent": 30}
        )
        is None
    )

    assert (await db.get(measurement_uuid=measurement_uuid, agent_uuid=agent_uuid))[
        "probing_statistics"
    ] == {"1:0:0": {"packets_sent": 10}, "2:0:0": {"packets_sent": 30}}

    assert (
        await db.stamp_canceled(
            measurement_uuid=measurement_uuid, agent_uuid=agent_uuid
        )
        is None
    )
    assert (await db.get(measurement_uuid=measurement_uuid, agent_uuid=agent_uuid))[
        "state"
    ] == "canceled"

    assert (
        await db.stamp_finished(
            measurement_uuid=measurement_uuid, agent_uuid=agent_uuid
        )
        is None
    )
    assert (await db.get(measurement_uuid=measurement_uuid, agent_uuid=agent_uuid))[
        "state"
    ] == "finished"
