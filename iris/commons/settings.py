import logging

from pydantic import BaseSettings


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
    DATABASE_MAX_BLOCK_SIZE: int = 8192
    DATABASE_CONNECT_TIMEOUT: int = 10
    DATABASE_SEND_RECEIVE_TIMEOUT: int = 300
    DATABASE_SYNC_REQUEST_TIMEOUT: int = 5
    DATABASE_TIMEOUT: int = 2 * 60 * 60  # in seconds
    DATABASE_TIMEOUT_EXPONENTIAL_MULTIPLIERS: int = 60  # in seconds
    DATABASE_TIMEOUT_EXPONENTIAL_MIN: int = 1  # in seconds
    DATABASE_TIMEOUT_EXPONENTIAL_MAX: int = 15 * 60  # in seconds
    DATABASE_TIMEOUT_RANDOM_MIN: int = 0  # in seconds
    DATABASE_TIMEOUT_RANDOM_MAX: int = 60  # in seconds
    DATABASE_PARALLEL_CSV_INSERT: bool = True  # linux/macos only
    DATABASE_PARALLEL_CSV_MAX_LINE: int = 25_000_000

    TABLE_NAME_USERS: str = f"{DATABASE_NAME}.users"
    TABLE_NAME_MEASUREMENTS: str = f"{DATABASE_NAME}.measurements"
    TABLE_NAME_AGENTS: str = f"{DATABASE_NAME}.agents"
    TABLE_NAME_AGENTS_SPECIFIC: str = f"{DATABASE_NAME}.agents_specific"

    REDIS_URL: str = "redis://redis"
    REDIS_HOSTNAME: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = "redispass"
    REDIS_SSL: bool = False
    REDIS_TIMEOUT: int = 2 * 60 * 60  # in seconds
    REDIS_TIMEOUT_EXPONENTIAL_MULTIPLIERS: int = 60  # in seconds
    REDIS_TIMEOUT_EXPONENTIAL_MIN: int = 1  # in seconds
    REDIS_TIMEOUT_EXPONENTIAL_MAX: int = 15 * 60  # in seconds
    REDIS_TIMEOUT_RANDOM_MIN: int = 0  # in seconds
    REDIS_TIMEOUT_RANDOM_MAX: int = 60  # in seconds

    LOKI_URL: str = "http://loki:3100/loki/api/v1/push"
    LOKI_USER: str = "admin"
    LOKI_PASSWORD: str = "admin"
    LOKI_VERSION: str = "1"
    LOKI_QUEUE_SIZE: int = 1000
    LOKI_LOGGING_LEVEL: int = logging.INFO

    STREAM_LOGGING_LEVEL: int = logging.DEBUG
