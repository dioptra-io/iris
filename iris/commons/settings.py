import logging
from datetime import timedelta
from functools import wraps

from pydantic import BaseSettings
from tenacity import retry
from tenacity.before_sleep import before_sleep_log
from tenacity.stop import stop_after_delay
from tenacity.wait import wait_random


class CommonSettings(BaseSettings):
    """Common settings."""

    CLICKHOUSE_BASE_URL: str = "http://clickhouse.docker.localhost"
    CLICKHOUSE_DATABASE: str = "iris"
    CLICKHOUSE_USERNAME: str = "iris"
    CLICKHOUSE_PASSWORD: str = "iris"
    CLICKHOUSE_PARALLEL_CSV_MAX_LINE: int = 25_000_000
    CLICKHOUSE_STORAGE_POLICY: str = "default"
    CLICKHOUSE_ARCHIVE_VOLUME: str = "default"
    CLICKHOUSE_ARCHIVE_INTERVAL: timedelta = timedelta(days=15)

    DATABASE_URL: str = "postgresql://iris:iris@postgres.docker.localhost/iris"

    REDIS_NAMESPACE: str = "iris"
    REDIS_URL: str = "redis://default:iris@redis.docker.localhost"

    RETRY_TIMEOUT: int = 2 * 60 * 60  # seconds, set to -1 to disable tenacity
    RETRY_TIMEOUT_RANDOM_MIN: int = 0  # seconds
    RETRY_TIMEOUT_RANDOM_MAX: int = 10 * 60  # seconds

    S3_ENDPOINT_URL: str = "http://minio.docker.localhost"
    S3_ACCESS_KEY_ID: str = "minioadmin"
    S3_SECRET_ACCESS_KEY: str = "minioadmin"
    S3_SESSION_TOKEN: str | None = None
    S3_REGION_NAME: str = "local"
    S3_PREFIX: str = "iris"

    STREAM_LOGGING_LEVEL: int = logging.INFO

    @property
    def clickhouse(self):
        return {
            "base_url": self.CLICKHOUSE_BASE_URL,
            "database": self.CLICKHOUSE_DATABASE,
            "username": self.CLICKHOUSE_USERNAME,
            "password": self.CLICKHOUSE_PASSWORD,
        }

    @property
    def s3(self):
        return {
            "aws_access_key_id": self.S3_ACCESS_KEY_ID,
            "aws_secret_access_key": self.S3_SECRET_ACCESS_KEY,
            "aws_session_token": self.S3_SESSION_TOKEN,
            "endpoint_url": self.S3_ENDPOINT_URL,
            "region_name": self.S3_REGION_NAME,
        }


def fault_tolerant(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        settings: CommonSettings = self.settings
        if settings.RETRY_TIMEOUT < 0:
            return func(self, *args, **kwargs)
        return retry(
            before_sleep=(before_sleep_log(self.logger, logging.ERROR)),
            stop=stop_after_delay(settings.RETRY_TIMEOUT),
            wait=wait_random(
                settings.RETRY_TIMEOUT_RANDOM_MIN,
                settings.RETRY_TIMEOUT_RANDOM_MAX,
            ),
        )(func)(self, *args, **kwargs)

    return wrapper
