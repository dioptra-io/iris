"""Agents settings."""

from pathlib import Path
from typing import Optional
from uuid import uuid4

from iris.commons.settings import CommonSettings


class AgentSettings(CommonSettings):
    """Agent specific settings."""

    SETTINGS_CLASS = "agent"

    AGENT_D_MINER_PROBER_PATH: Path = Path("/usr/bin/diamond-miner-prober")
    AGENT_D_MINER_EXCLUDE_PATH: Path = Path("/app/statics/excluded_prefixes")
    AGENT_D_MINER_BGP_PREFIXES: Optional[str] = None

    AGENT_UUID: str = str(uuid4())

    AGENT_PROBING_RATE: int = 1000  # pps
    AGENT_BUFFER_SNIFFER_SIZE: int = 100000  # kB

    AGENT_INF_BORN: int = 0
    AGENT_SUP_BORN: int = (2 ** 32) - 1

    AGENT_IPS_PER_SUBNET: int = 6

    AGENT_TARGETS_DIR_PATH: Path = Path("/app/targets")
    AGENT_RESULTS_DIR_PATH: Path = Path("/app/results")

    AGENT_WAIT_FOR_START: int = 10  # seconds
    AGENT_RECOVER_TIME_REDIS_FAILURE: int = 10  # seconds

    WORKER_STOPPER_REFRESH: int = 1  # seconds

    AGENT_DEBUG_MODE: bool = False
