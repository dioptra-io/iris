"""Agents settings."""

import logging
from enum import Enum
from ipaddress import IPv4Address
from pathlib import Path
from typing import List
from uuid import uuid4

from iris.commons.settings import CommonSettings


class RateLimitingMethod(str, Enum):
    auto = "auto"
    active = "active"
    sleep = "sleep"
    none = "none"


class AgentSettings(CommonSettings):
    """Agent specific settings."""

    AGENT_CARACAL_EXCLUDE_PATH: Path = Path("statics/excluded_prefixes")
    AGENT_CARACAL_RATE_LIMITING_METHOD: RateLimitingMethod = RateLimitingMethod.auto
    AGENT_CARACAL_SNIFFER_WAIT_TIME: int = 5
    AGENT_CARACAL_LOGGING_LEVEL: int = logging.INFO
    AGENT_CARACAL_INTEGRITY_CHECK: bool = True

    AGENT_UUID: str = str(uuid4())
    AGENT_MAX_PROBING_RATE: int = 1000  # pps
    AGENT_MIN_TTL: int = -1  # A value < 0 will trigger `find_exit_ttl`
    AGENT_MIN_TTL_FIND_TARGET: str = "8.8.8.8"
    AGENT_TAGS: List[str] = ["all"]

    AGENT_TARGETS_DIR_PATH: Path = Path("iris_data/agent/targets")
    AGENT_RESULTS_DIR_PATH: Path = Path("iris_data/agent/results")

    AGENT_STOPPER_REFRESH: int = 1  # seconds
    AGENT_DEBUG_MODE: bool = False
