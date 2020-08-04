from iris.commons.settings import CommonSettings
from pathlib import Path


class WorkerSettings(CommonSettings):
    """Worker specific settings."""

    WORKER_D_MINER_READER_PATH: Path = Path("/app/reader/build/Reader")
    WORKER_EXCLUSION_FILE_PATH: Path = Path("/app/reader/resources/excluded_prefixes")

    WORKER_RESULTS_DIR_PATH: Path = Path("/app/results")

    WORKER_TERASHUF_PATH: Path = Path("/app/terashuf/terashuf")
    WORKER_TERASHUF_MEMORY: int = 40  # GB
    WORKER_TERASHUF_TMP_DIR_PATH: Path = Path("/app/reader/resources/")

    WORKER_TIME_LIMIT: int = 60 * 60 * 1000  # seconds (1hour)
    WORKER_MESSAGE_AGE_LIMIT: int = 60 * 60 * 1000  # seconds (1hour)

    WORKER_WATCH_REFRESH: int = 1  # seconds

    WORKER_SANITY_CHECK_ENABLE: bool = True
    WORKER_SANITY_CHECK_RETRIES: int = 3
    WORKER_SANITY_CHECK_REFRESH: int = 1  # seconds

    WORKER_DEBUG_MODE: bool = False
