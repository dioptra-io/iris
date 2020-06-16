from pydantic import BaseSettings


class Settings(BaseSettings):

    AWS_S3_HOST: str = "http://minio:9000"
    AWS_ACCESS_KEY_ID: str = "minioadmin"
    AWS_SECRET_ACCESS_KEY: str = "minioadmin"
    AWS_REGION_NAME: str = "local"
    AWS_S3_TARGETS_BUCKET_NAME = "targets"

    D_MINER_EXECUTABLE_PATH = "/app/prober/build/Heartbeat"

    WATCHER_TIMEOUT: int = 60 * 60 * 1000
