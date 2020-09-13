from pydantic import BaseSettings


class CommonSettings(BaseSettings):
    """Common settings."""

    AWS_S3_HOST: str = "http://minio:9000"
    AWS_ACCESS_KEY_ID: str = "minioadmin"
    AWS_SECRET_ACCESS_KEY: str = "minioadmin"
    AWS_REGION_NAME: str = "local"
    AWS_S3_TARGETS_BUCKET_NAME = "targets"
    AWS_TIMEOUT: int = 2 * 60 * 60  # in seconds
    AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS: int = 60  # in seconds
    AWS_TIMEOUT_EXPONENTIAL_MIN: int = 1  # in seconds
    AWS_TIMEOUT_EXPONENTIAL_MAX: int = 15 * 60  # in seconds

    DATABASE_HOST: str = "clickhouse"
    DATABASE_NAME: str = "iris"

    TABLE_NAME_MEASUREMENTS: str = f"{DATABASE_NAME}.measurements"
    TABLE_NAME_AGENTS: str = f"{DATABASE_NAME}.agents"
    TABLE_NAME_AGENTS_SPECIFIC: str = f"{DATABASE_NAME}.agents_specific"

    REDIS_URL: str = "redis://redis"
    REDIS_HOSTNAME: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = "redispass"

    LOKI_URL: str = "http://loki:3100/loki/api/v1/push"
    LOKI_VERSION: str = "1"
