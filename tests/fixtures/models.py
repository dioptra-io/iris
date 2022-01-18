from datetime import datetime
from uuid import uuid4

import pytest

from iris import __version__
from iris.commons.models.agent import AgentParameters
from iris.commons.models.diamond_miner import ProbingStatistics, Tool, ToolParameters
from iris.commons.models.measurement import Measurement
from iris.commons.models.measurement_agent import MeasurementAgent
from iris.commons.models.round import Round
from iris.commons.models.user import User


@pytest.fixture
def make_agent_parameters():
    def _make_agent_parameters(**kwargs):
        default = dict(
            version=__version__,
            hostname="agent.example.org",
            min_ttl=0,
            max_probing_rate=100,
        )
        return AgentParameters(**{**default, **kwargs})

    return _make_agent_parameters


@pytest.fixture
def make_measurement_agent(make_agent_parameters):
    def _make_measurement_agent(**kwargs):
        default = dict(
            agent_uuid=str(uuid4()),
            agent_parameters=make_agent_parameters(),
            tool_parameters=ToolParameters(),
            target_file=f"{uuid4()}.csv",
        )
        return MeasurementAgent(**{**default, **kwargs})

    return _make_measurement_agent


@pytest.fixture
def make_measurement(make_measurement_agent):
    def _make_measurement(**kwargs):
        default = dict(
            tool=Tool.DiamondMiner,
            tags=["tag1", "tag2"],
            agents=[make_measurement_agent()],
        )
        return Measurement(**{**default, **kwargs})

    return _make_measurement


@pytest.fixture
def make_user():
    def _make_user(**kwargs):
        default = dict(
            email="user@example.org",
            allow_tag_public=True,
            allow_tag_reserved=True,
            is_active=True,
            is_verified=True,
            probing_limit=1_000_000,
        )
        return User(**{**default, **kwargs})

    return _make_user


@pytest.fixture
def make_probing_statistics():
    def _make_probing_statistics(**kwargs):
        default = dict(
            round=Round(number=1, limit=10, offset=0),
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow(),
            filtered_low_ttl=0,
            filtered_high_ttl=0,
            filtered_prefix_excl=0,
            filtered_prefix_not_incl=0,
            probes_read=0,
            packets_sent=0,
            packets_failed=0,
            packets_received=0,
            packets_received_invalid=0,
            pcap_received=0,
            pcap_dropped=0,
            pcap_interface_dropped=0,
        )
        return ProbingStatistics(**{**default, **kwargs})

    return _make_probing_statistics
