from pydantic import BaseSettings


class CommonSettings(BaseSettings):
    """Common settings."""

    AWS_S3_HOST: str = "http://minio:9000"
    AWS_ACCESS_KEY_ID: str = "minioadmin"
    AWS_SECRET_ACCESS_KEY: str = "minioadmin"
    AWS_REGION_NAME: str = "local"
    AWS_S3_TARGETS_BUCKET_NAME = "targets"

    DATABASE_HOST: str = "clickhouse"
    DATABASE_NAME: str = "iris"

    MEASUREMENTS_TABLE_NAME: str = f"{DATABASE_NAME}.measurements"
    AGENTS_TABLE_NAME: str = f"{DATABASE_NAME}.agents"

    REDIS_URL: str = "redis://redis"
    REDIS_HOSTNAME: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_PASSWORD: str = "redispass"

    LOKI_URL: str = "http://loki:3100/loki/api/v1/push"
    LOKI_VERSION: str = "1"
