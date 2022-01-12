import logging
from datetime import timedelta
from functools import wraps
from typing import List, Optional

import aioredis
from pydantic import BaseSettings
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.future import Engine
from tenacity import retry
from tenacity.before_sleep import before_sleep_log
from tenacity.stop import stop_after_delay
from tenacity.wait import wait_random


def fault_tolerant(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        settings: CommonSettings = self.settings
        if settings.RETRY_TIMEOUT < 0:
            return await func(self, *args, **kwargs)
        return await retry(
            before_sleep=(before_sleep_log(self.logger, logging.ERROR)),
            stop=stop_after_delay(settings.RETRY_TIMEOUT),
            wait=wait_random(
                settings.RETRY_TIMEOUT_RANDOM_MIN,
                settings.RETRY_TIMEOUT_RANDOM_MAX,
            ),
        )(func)(self, *args, **kwargs)

    return wrapper


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
    REDIS_URL: str = "redis://default:redispass@redis.docker.localhost"

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

    sqlalchemy_engine_: Optional[Engine] = None
    sqlalchemy_async_engine_: Optional[AsyncEngine] = None

    def sqlalchemy_engine(self) -> Engine:
        if not self.sqlalchemy_engine_:
            self.sqlalchemy_engine_ = create_engine(
                self.DATABASE_URL,
                connect_args=dict(connect_timeout=5),
                echo=True,
                future=True,
            )
        return self.sqlalchemy_engine_

    def sqlalchemy_async_engine(self) -> AsyncEngine:
        if not self.sqlalchemy_async_engine_:
            self.sqlalchemy_async_engine_ = create_async_engine(
                self.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://"),
                connect_args=dict(command_timeout=5, timeout=5),
                echo=True,
                future=True,
            )
        return self.sqlalchemy_async_engine_

    async def redis_client(self) -> aioredis.Redis:
        redis: aioredis.Redis = await aioredis.from_url(
            self.REDIS_URL, decode_responses=True
        )
        return redis
