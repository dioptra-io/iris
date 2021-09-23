import logging
import platform
from functools import wraps
from typing import Optional

import aioredis
from pydantic import BaseSettings
from sqlalchemy.future import Engine
from sqlmodel import create_engine
from tenacity import (  # type: ignore
    before_sleep_log,
    retry,
    stop_after_delay,
    wait_exponential,
    wait_random,
)


def fault_tolerant(retry_):
    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            # Retrieve logger and settings objects from
            # the instance to which the function belongs.
            parent = self
            if hasattr(parent, "database"):
                parent = parent.database
            return await retry_(parent.settings, parent.logger)(func)(
                self, *args, **kwargs
            )

        return wrapper

    return decorator


class CommonSettings(BaseSettings):
    """Common settings."""

    SETTINGS_CLASS = "commons"

    AWS_S3_HOST: str = "http://minio:9000"
    AWS_ACCESS_KEY_ID: str = "minioadmin"
    AWS_SECRET_ACCESS_KEY: str = "minioadmin"
    AWS_REGION_NAME: str = "local"
    AWS_S3_ARCHIVE_BUCKET_PREFIX = "archive-"
    AWS_S3_TARGETS_BUCKET_PREFIX = "targets-"
    AWS_TIMEOUT: int = 2 * 60 * 60  # in seconds
    AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS: int = 60  # in seconds
    AWS_TIMEOUT_EXPONENTIAL_MIN: int = 1  # in seconds
    AWS_TIMEOUT_EXPONENTIAL_MAX: int = 15 * 60  # in seconds
    AWS_TIMEOUT_RANDOM_MIN: int = 0  # in seconds
    AWS_TIMEOUT_RANDOM_MAX: int = 10 * 60  # in seconds

    DATABASE_HOST: str = "clickhouse"
    DATABASE_NAME: str = "iris"
    DATABASE_CONNECT_TIMEOUT: int = 10
    DATABASE_SEND_RECEIVE_TIMEOUT: int = 300
    DATABASE_SYNC_REQUEST_TIMEOUT: int = 5
    DATABASE_TIMEOUT: int = 2 * 60 * 60  # in seconds
    DATABASE_TIMEOUT_EXPONENTIAL_MULTIPLIERS: int = 60  # in seconds
    DATABASE_TIMEOUT_EXPONENTIAL_MIN: int = 1  # in seconds
    DATABASE_TIMEOUT_EXPONENTIAL_MAX: int = 15 * 60  # in seconds
    DATABASE_TIMEOUT_RANDOM_MIN: int = 0  # in seconds
    DATABASE_TIMEOUT_RANDOM_MAX: int = 60  # in seconds
    DATABASE_PARALLEL_CSV_MAX_LINE: int = 25_000_000

    TABLE_NAME_USERS: str = "users"
    TABLE_NAME_MEASUREMENTS: str = "measurements"
    TABLE_NAME_AGENTS: str = "agents"

    REDIS_URL: str = "redis://default:redispass@redis"
    REDIS_TIMEOUT: int = 2 * 60 * 60  # in seconds
    REDIS_TIMEOUT_EXPONENTIAL_MULTIPLIERS: int = 60  # in seconds
    REDIS_TIMEOUT_EXPONENTIAL_MIN: int = 1  # in seconds
    REDIS_TIMEOUT_EXPONENTIAL_MAX: int = 15 * 60  # in seconds
    REDIS_TIMEOUT_RANDOM_MIN: int = 0  # in seconds
    REDIS_TIMEOUT_RANDOM_MAX: int = 5  # in seconds

    STREAM_LOGGING_LEVEL: int = logging.DEBUG

    CLICKHOUSE_CMD: str = "clickhouse client"
    SPLIT_CMD: str = "gsplit" if platform.system() == "Darwin" else "split"
    ZSTD_CMD: str = "zstd"

    SQLMODEL_DATABASE_URL: str = "sqlite:///iris.sqlite3"
    sqlmodel_engine_: Optional[Engine] = None

    def database_url(self, default: bool = False) -> str:
        """Return the ClickHouse URL."""
        host = self.DATABASE_HOST
        database = self.DATABASE_NAME if not default else "default"
        url = f"clickhouse://{host}/{database}"
        url += f"?connect_timeout={self.DATABASE_CONNECT_TIMEOUT}"
        url += f"&send_receive_timeout={self.DATABASE_SEND_RECEIVE_TIMEOUT}"
        url += f"&sync_request_timeout={self.DATABASE_SYNC_REQUEST_TIMEOUT}"
        return url

    def sqlmodel_engine(self) -> Engine:
        if not self.sqlmodel_engine_:
            self.sqlmodel_engine_ = create_engine(
                self.SQLMODEL_DATABASE_URL,
                connect_args={"check_same_thread": False},
                echo=True,
            )
        return self.sqlmodel_engine_

    async def redis_client(self) -> aioredis.Redis:
        redis: aioredis.Redis = await aioredis.from_url(
            self.REDIS_URL, decode_responses=True
        )
        return redis

    def database_retry(self, logger):
        return retry(
            stop=stop_after_delay(self.DATABASE_TIMEOUT),
            wait=wait_exponential(
                multiplier=self.DATABASE_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
                min=self.DATABASE_TIMEOUT_EXPONENTIAL_MIN,
                max=self.DATABASE_TIMEOUT_EXPONENTIAL_MAX,
            )
            + wait_random(
                self.DATABASE_TIMEOUT_RANDOM_MIN,
                self.DATABASE_TIMEOUT_RANDOM_MAX,
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
