from diamond_miner.commons.settings import CommonSettings


class WorkerSettings(CommonSettings):
    """Worker specific settings."""

    WATCHER_TIMEOUT: int = 60 * 60 * 1000
