import logging
from datetime import timedelta
from functools import wraps
from typing import List, Optional

from pydantic import BaseSettings
from tenacity import retry
from tenacity.before_sleep import before_sleep_log
from tenacity.stop import stop_after_delay
from tenacity.wait import wait_random


class CommonSettings(BaseSettings):
    """Common settings."""

    CHPROXY_PUBLIC_URL: str = ""
    CHPROXY_PUBLIC_USERNAME: str = ""
    CHPROXY_PUBLIC_PASSWORD: str = ""

    CLICKHOUSE_URL: str = "http://iris:iris@clickhouse.docker.localhost/?database=iris"
    CLICKHOUSE_PUBLIC_USER: Optional[str] = None
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

    S3_HOST: str = "http://minio.docker.localhost"
    S3_ACCESS_KEY_ID: str = "minioadmin"
    S3_SECRET_ACCESS_KEY: str = "minioadmin"
    S3_SESSION_TOKEN: Optional[str] = None
    S3_REGION_NAME: str = "local"

    S3_ARCHIVE_BUCKET_PREFIX = "archive-"
    S3_TARGETS_BUCKET_PREFIX = "targets-"

    S3_PUBLIC_ACTIONS: List[str] = [
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
    ]
    S3_PUBLIC_RESOURCES: List[str] = [
        "arn:aws:s3:::public-exports",
        "arn:aws:s3:::public-exports/*",
    ]

    STREAM_LOGGING_LEVEL: int = logging.INFO

    TAG_PUBLIC: str = "!public"
    TAG_COLLECTION_PREFIX: str = "collection:"


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
