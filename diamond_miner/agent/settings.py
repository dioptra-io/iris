from diamond_miner.commons.settings import CommonSettings
from pathlib import Path


class AgentSettings(CommonSettings):
    """Agent specific settings."""

    AGENT_D_MINER_PROBER_PATH: Path = Path("/app/prober/build/Heartbeat")
    AGENT_PROBING_RATE: int = 1000
    AGENT_BUFFER_SNIFFER_SIZE: int = 100000

    AGENT_INF_BORN: int = 0
    AGENT_SUP_BORN: int = (2 ** 32) - 1

    AGENT_IPS_PER_SUBNET: int = 6

    AGENT_TARGETS_DIR: Path = Path("/app/targets")
    AGENT_RESULTS_DIR: Path = Path("/app/results")
