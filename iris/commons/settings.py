import logging
from datetime import timedelta
from functools import wraps
from pathlib import Path
from typing import List, Optional

import aioredis
from pydantic import BaseSettings
from sqlalchemy import create_engine
from sqlalchemy.future import Engine
from tenacity import retry
from tenacity.before_sleep import before_sleep_log
from tenacity.stop import stop_after_delay
from tenacity.wait import wait_exponential, wait_random


def fault_tolerant(retry_):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Retrieve logger and settings objects from
            # the instance to which the function belongs.
            parent = self
            return await retry_(parent.settings, parent.logger)(func)(
                self, *args, **kwargs
            )

        return wrapper

    return decorator


class CommonSettings(BaseSettings):
    """Common settings."""

    AWS_S3_HOST: str = "http://minio.docker.localhost"
    AWS_ACCESS_KEY_ID: str = "minioadmin"
    AWS_SECRET_ACCESS_KEY: str = "minioadmin"
    AWS_SESSION_TOKEN: Optional[str] = None
    AWS_REGION_NAME: str = "local"
    AWS_S3_ARCHIVE_BUCKET_PREFIX = "archive-"
    AWS_S3_TARGETS_BUCKET_PREFIX = "targets-"
    AWS_PUBLIC_ACTIONS: List[str] = [
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
    ]
    AWS_PUBLIC_RESOURCES: List[str] = [
        "arn:aws:s3:::public-exports",
        "arn:aws:s3:::public-exports/*",
    ]
    AWS_TIMEOUT: int = 2 * 60 * 60  # in seconds
    AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS: int = 60  # in seconds
    AWS_TIMEOUT_EXPONENTIAL_MIN: int = 1  # in seconds
    AWS_TIMEOUT_EXPONENTIAL_MAX: int = 15 * 60  # in seconds
    AWS_TIMEOUT_RANDOM_MIN: int = 0  # in seconds
    AWS_TIMEOUT_RANDOM_MAX: int = 10 * 60  # in seconds

    CHPROXY_PUBLIC_URL: str = ""
    CHPROXY_PUBLIC_USERNAME: str = ""
    CHPROXY_PUBLIC_PASSWORD: str = ""

    CLICKHOUSE_URL: str = "http://iris:iris@clickhouse.docker.localhost/?database=iris"
    CLICKHOUSE_PUBLIC_USER: Optional[str] = None
    CLICKHOUSE_TIMEOUT: int = 2 * 60 * 60  # in seconds
    CLICKHOUSE_TIMEOUT_EXPONENTIAL_MULTIPLIERS: int = 60  # in seconds
    CLICKHOUSE_TIMEOUT_EXPONENTIAL_MIN: int = 1  # in seconds
    CLICKHOUSE_TIMEOUT_EXPONENTIAL_MAX: int = 15 * 60  # in seconds
    CLICKHOUSE_TIMEOUT_RANDOM_MIN: int = 0  # in seconds
    CLICKHOUSE_TIMEOUT_RANDOM_MAX: int = 60  # in seconds
    CLICKHOUSE_PARALLEL_CSV_MAX_LINE: int = 25_000_000
    CLICKHOUSE_STORAGE_POLICY: str = "default"
    CLICKHOUSE_ARCHIVE_VOLUME: str = "default"
    CLICKHOUSE_ARCHIVE_INTERVAL: timedelta = timedelta(days=15)

    TABLE_NAME_USERS: str = "users"
    TABLE_NAME_MEASUREMENTS: str = "measurements"
    TABLE_NAME_AGENTS: str = "agents"

    REDIS_NAMESPACE: str = "iris"
    REDIS_URL: str = "redis://default:redispass@redis.docker.localhost"
    REDIS_TIMEOUT: int = 2 * 60 * 60  # in seconds
    REDIS_TIMEOUT_EXPONENTIAL_MULTIPLIERS: int = 60  # in seconds
    REDIS_TIMEOUT_EXPONENTIAL_MIN: int = 1  # in seconds
    REDIS_TIMEOUT_EXPONENTIAL_MAX: int = 15 * 60  # in seconds
    REDIS_TIMEOUT_RANDOM_MIN: int = 0  # in seconds
    REDIS_TIMEOUT_RANDOM_MAX: int = 5  # in seconds

    STREAM_LOGGING_LEVEL: int = logging.DEBUG

    SQLALCHEMY_DATABASE_URL: str = "sqlite:///iris_data/iris.sqlite3"
    sqlalchemy_engine_: Optional[Engine] = None

    def sqlalchemy_engine(self) -> Engine:
        if not self.sqlalchemy_engine_:
            self.sqlalchemy_engine_ = create_engine(
                self.SQLALCHEMY_DATABASE_URL,
                connect_args={"check_same_thread": False},
                echo=True,
                future=True,
            )
        return self.sqlalchemy_engine_

    def sqlalchemy_database_path(self) -> Path:
        # Remove sqlite:///
        # TODO: Use .removeprefix with Python 3.9+
        return Path(self.SQLALCHEMY_DATABASE_URL[10:])

    async def redis_client(self) -> aioredis.Redis:
        redis: aioredis.Redis = await aioredis.from_url(
            self.REDIS_URL, decode_responses=True
        )
        return redis

    def database_retry(self, logger):
        return retry(
            stop=stop_after_delay(self.CLICKHOUSE_TIMEOUT),
            wait=wait_exponential(
                multiplier=self.CLICKHOUSE_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
                min=self.CLICKHOUSE_TIMEOUT_EXPONENTIAL_MIN,
                max=self.CLICKHOUSE_TIMEOUT_EXPONENTIAL_MAX,
            )
            + wait_random(
                self.CLICKHOUSE_TIMEOUT_RANDOM_MIN,
                self.CLICKHOUSE_TIMEOUT_RANDOM_MAX,
            ),
            before_sleep=(before_sleep_log(logger, logging.ERROR)),
        )

    def redis_retry(self, logger):
        return retry(
            stop=stop_after_delay(self.REDIS_TIMEOUT),
            wait=wait_exponential(
                multiplier=self.REDIS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
                min=self.REDIS_TIMEOUT_EXPONENTIAL_MIN,
                max=self.REDIS_TIMEOUT_EXPONENTIAL_MAX,
            )
            + wait_random(
                self.REDIS_TIMEOUT_RANDOM_MIN,
                self.REDIS_TIMEOUT_RANDOM_MAX,
            ),
            before_sleep=(before_sleep_log(logger, logging.ERROR)),
        )

    def storage_retry(self, logger):
        return retry(
            stop=stop_after_delay(self.AWS_TIMEOUT),
            wait=wait_exponential(
                multiplier=self.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
                min=self.AWS_TIMEOUT_EXPONENTIAL_MIN,
                max=self.AWS_TIMEOUT_EXPONENTIAL_MAX,
            )
            + wait_random(
                self.AWS_TIMEOUT_RANDOM_MIN,
                self.AWS_TIMEOUT_RANDOM_MAX,
            ),
            before_sleep=(before_sleep_log(logger, logging.ERROR)),
        )
