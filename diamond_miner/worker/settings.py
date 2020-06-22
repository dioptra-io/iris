from diamond_miner.commons.settings import CommonSettings
from pathlib import Path


class WorkerSettings(CommonSettings):
    """Worker specific settings."""

    WORKER_D_MINER_READER_PATH: Path = Path("/app/reader/build/Reader")
    WORKER_EXCLUSION_FILE_PATH: Path = Path("/app/reader/resources/excluded_prefixes")
    WORKER_TIME_LIMIT: int = 60 * 60 * 1000  # 1hour
    WORKER_MESSAGE_AGE_LIMIT: int = 60 * 60 * 1000  # 1hour

    WORKER_WATCH_REFRESH: int = 1

    WORKER_RESULTS_DIR: Path = Path("/app/results")

    WORKER_DATABASE_HOST: str = "clickhouse"
    WORKER_DATABASE_NAME: str = "diamond_miner"
