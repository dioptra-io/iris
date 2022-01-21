from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import Field, NonNegativeInt

from iris.commons.models.base import BaseModel
from iris.commons.models.round import Round


class FlowMapper(Enum):
    SequentialFlowMapper = "SequentialFlowMapper"
    IntervalFlowMapper = "IntervalFlowMapper"
    ReverseByteFlowMapper = "ReverseByteFlowMapper"
    RandomFlowMapper = "RandomFlowMapper"


class Tool(Enum):
    DiamondMiner = "diamond-miner"
    Yarrp = "yarrp"
    Ping = "ping"
    Probes = "probes"


class ToolParameters(BaseModel):
    initial_source_port: int = Field(
        24000, title="Initial source port", gt=0, lt=65_536
    )
    destination_port: int = Field(33434, title="Destination port", gt=0, lt=65_536)
    max_round: int = Field(10, title="Maximum round", gt=0, lt=256)
    failure_probability: float = Field(
        0.05,
        title="Diamond-Miner failure probability",
        description="Ignored for other tools",
    )
    flow_mapper: FlowMapper = Field(FlowMapper.RandomFlowMapper, title="Flow mapper")
    flow_mapper_kwargs: Optional[Dict[str, Any]] = Field(
        {"seed": 42}, title="Flow mapper optional arguments"
    )
    prefix_len_v4: int = Field(24, ge=0, le=32, title="Target prefix length (IPv4)")
    prefix_len_v6: int = Field(64, ge=0, le=128, title="Target prefix length (IPv6)")
    global_min_ttl: int = Field(
        0,
        ge=0,
        le=255,
        title="Global minimum TTL",
        description="Do not set. Overridden by the API",
    )
    global_max_ttl: int = Field(
        255,
        ge=0,
        le=255,
        title="Global maximum TTL",
        description="Do not set. Overridden by the API",
    )

    @property
    def prefix_size_v4(self):
        return 2 ** (32 - self.prefix_len_v4)

    @property
    def prefix_size_v6(self):
        return 2 ** (128 - self.prefix_len_v6)


class ProbingStatistics(BaseModel):
    round: Round
    start_time: datetime
    end_time: datetime
    filtered_low_ttl: NonNegativeInt
    filtered_high_ttl: NonNegativeInt
    filtered_prefix_excl: NonNegativeInt
    filtered_prefix_not_incl: NonNegativeInt
    probes_read: NonNegativeInt
    packets_sent: NonNegativeInt
    packets_failed: NonNegativeInt
    packets_received: NonNegativeInt
    packets_received_invalid: NonNegativeInt
    pcap_received: NonNegativeInt
    pcap_dropped: NonNegativeInt
    pcap_interface_dropped: NonNegativeInt
