from diamond_miner.commons.settings import CommonSettings
from pathlib import Path


class WorkerSettings(CommonSettings):
    """Worker specific settings."""

    WORKER_D_MINER_READER_PATH: Path = Path("/app/reader/build/Reader")
    WORKER_TIMEOUT: int = 60 * 60 * 1000

    WORKER_WATCH_REFRESH: int = 1

    WORKER_RESULTS_DIR: Path = Path("/app/results")
