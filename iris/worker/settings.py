from pathlib import Path

from iris.commons.settings import CommonSettings


class WorkerSettings(CommonSettings):
    """Worker specific settings."""

    SETTINGS_CLASS = "worker"

    WORKER_RESULTS_DIR_PATH: Path = Path("iris_data/worker/results")

    WORKER_TIME_LIMIT: int = 60 * 60 * 1000  # seconds (1hour)
    WORKER_MESSAGE_AGE_LIMIT: int = 60 * 60 * 1000  # seconds (1hour)

    WORKER_SANITY_CHECK_ENABLE: bool = True
    WORKER_SANITY_CHECK_RETRIES: int = 3
    WORKER_SANITY_CHECK_REFRESH_MIN: int = 1  # seconds
    WORKER_SANITY_CHECK_REFRESH_MAX: int = 10  # seconds

    WORKER_ROUND_1_SLIDING_WINDOW: int = 10  # put to 0 to deactivate sliding window
    WORKER_ROUND_1_STOPPING: int = (
        3  # stops probing a prefix if more than this number of *
    )

    WORKER_DEBUG_MODE: bool = False
