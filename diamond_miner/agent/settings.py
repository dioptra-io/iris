from diamond_miner.commons.settings import CommonSettings


class AgentSettings(CommonSettings):
    """Agent specific settings."""

    AGENT_D_MINER_EXE_PATH = "/app/prober/build/Heartbeat"
    AGENT_PROBING_RATE = 1000
    AGENT_BUFFER_SNIFFER_SIZE = 100000

    AGENT_INF_BORN = 0
    AGENT_SUP_BORN = (2 ** 32) - 1

    AGENT_IPS_PER_SUBNET = 6
