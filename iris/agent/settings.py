from iris.commons.settings import CommonSettings
from typing import Optional
from pathlib import Path


class AgentSettings(CommonSettings):
    """Agent specific settings."""

    AGENT_D_MINER_PROBER_PATH: Path = Path("/app/diamond-miner-prober")
    AGENT_D_MINER_EXCLUDE_PATH: Path = None

    AGENT_UUID: Optional[str] = None

    AGENT_PROBING_RATE: int = 1000  # pps
    AGENT_BUFFER_SNIFFER_SIZE: int = 100000  # kB

    AGENT_PFRING: bool = False

    AGENT_TARGETS_DIR_PATH: Path = Path("/app/targets")
    AGENT_RESULTS_DIR_PATH: Path = Path("/app/results")

    AGENT_WAIT_FOR_START: int = 10  # seconds
    AGENT_RECOVER_TIME_REDIS_FAILURE: int = 10  # seconds

    WORKER_STOPPER_REFRESH: int = 1  # seconds

    AGENT_DEBUG_MODE: bool = False
