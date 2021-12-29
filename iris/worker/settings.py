from pathlib import Path

from iris.commons.settings import CommonSettings


class WorkerSettings(CommonSettings):
    """Worker specific settings."""

    WORKER_RESULTS_DIR_PATH: Path = Path("iris_data/worker/results")

    WORKER_TIME_LIMIT: int = 60 * 60 * 1000  # seconds (1hour)
    WORKER_MESSAGE_AGE_LIMIT: int = 60 * 60 * 1000  # seconds (1hour)

    WORKER_SANITY_CHECK_RETRIES: int = 3
    WORKER_SANITY_CHECK_INTERVAL: float = 1  # seconds
    WORKER_WATCH_INTERVAL: float = 2  # seconds

    WORKER_ROUND_1_SLIDING_WINDOW: int = 10  # put to 0 to deactivate sliding window
    WORKER_ROUND_1_STOPPING: int = (
        3  # stops probing a prefix if more than this number of *
    )

    WORKER_DEBUG_MODE: bool = False
