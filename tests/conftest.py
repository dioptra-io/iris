import logging
import os
import secrets

import boto3
import pytest
import redis as pyredis
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy_utils import create_database, database_exists, drop_database
from sqlmodel import Session, create_engine

from iris.agent.settings import AgentSettings
from iris.api.authentication import (
    current_active_user,
    current_superuser,
    current_verified_user,
)
from iris.api.main import app
from iris.api.settings import APISettings
from iris.commons.clickhouse import ClickHouse
from iris.commons.dependencies import get_settings
from iris.commons.models.base import Base
from iris.commons.redis import Redis
from iris.commons.settings import CommonSettings
from iris.commons.storage import Storage
from iris.worker import WorkerSettings

pytest.register_assert_rewrite("tests.assertions")
pytest_plugins = ["tests.fixtures.models", "tests.fixtures.storage"]


def should_cleanup():
    return os.environ.get("IRIS_TEST_CLEANUP", "") != "0"


@pytest.fixture
def logger():
    return logging.getLogger(__name__)


@pytest.fixture
def settings():
    namespace = secrets.token_hex(nbytes=4)
    print(f"@{namespace}", end=" ")
    # Redis has 16 databases by default, we use the last one for testing.
    return CommonSettings(
        CLICKHOUSE_PUBLIC_USER="public",
        CLICKHOUSE_URL="http://iris:iris@clickhouse.docker.localhost/?database=iris_test",
        DATABASE_URL=f"postgresql://iris:iris@postgres.docker.localhost/iris-test-{namespace}",
        S3_PUBLIC_RESOURCES=["arn:aws:s3:::test-public-exports/*"],
        S3_ARCHIVE_BUCKET_PREFIX=f"archive-test-{namespace}-",
        S3_TARGETS_BUCKET_PREFIX=f"targets-test-{namespace}-",
        REDIS_NAMESPACE=f"iris-test-{namespace}",
        REDIS_URL="redis://default:iris@redis.docker.localhost?db=15",
        RETRY_TIMEOUT=-1,
    )


@pytest.fixture
def api_settings(settings):
    return APISettings(**settings.dict())


@pytest.fixture
def agent_settings(settings, tmp_path):
    return AgentSettings(
        **settings.dict(),
        AGENT_CARACAL_SNIFFER_WAIT_TIME=1,
        AGENT_MIN_TTL=0,
        AGENT_RESULTS_DIR_PATH=tmp_path / "agent_results",
        AGENT_TARGETS_DIR_PATH=tmp_path / "agent_targets",
    )


@pytest.fixture
def worker_settings(settings, tmp_path):
    return WorkerSettings(
        **settings.dict(),
        WORKER_RESULTS_DIR_PATH=tmp_path / "worker_results",
        WORKER_MAX_OPEN_FILES=128,
    )


@pytest.fixture
def clickhouse(settings, logger):
    return ClickHouse(settings, logger)


@pytest.fixture
def engine(settings):
    engine = create_engine(settings.DATABASE_URL)
    if not database_exists(engine.url):
        create_database(engine.url)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
async def redis(settings, logger):
    client = await settings.redis_client()
    yield Redis(client, settings, logger)
    await client.close()


@pytest.fixture
def session(engine):
    with Session(engine) as session:
        yield session


@pytest.fixture
def storage(settings, logger):
    return Storage(settings, logger)


@pytest.fixture
def make_client(engine, settings):
    def _make_client(user=None):
        if user and user.is_active:
            app.dependency_overrides[current_active_user] = lambda: user
        if user and user.is_active and user.is_verified:
            app.dependency_overrides[current_verified_user] = lambda: user
        if user and user.is_active and user.is_verified and user.is_superuser:
            app.dependency_overrides[current_superuser] = lambda: user
        app.dependency_overrides[get_settings] = lambda: APISettings(**settings.dict())
        return TestClient(app)

    yield _make_client
    app.dependency_overrides.clear()


@pytest.fixture(autouse=True, scope="session")
def cleanup_redis():
    yield
    if should_cleanup():
        redis_ = pyredis.from_url("redis://default:iris@redis.docker.localhost?db=15")
        redis_.flushdb()
        redis_.close()


@pytest.fixture(autouse=True, scope="session")
def cleanup_database():
    yield
    if should_cleanup():
        # TODO: Cleanup/simplify this code.
        engine = create_engine("postgresql://iris:iris@postgres.docker.localhost")
        with engine.connect() as conn:
            databases = conn.execute(
                text(
                    """
                        SELECT datname
                        FROM pg_database
                        WHERE datistemplate = false AND datname LIKE 'iris-test-%'
                    """
                )
            ).all()
        for (database,) in databases:
            drop_database(
                f"postgresql://iris:iris@postgres.docker.localhost/{database}"
            )


@pytest.fixture(autouse=True, scope="session")
def cleanup_s3():
    yield
    if should_cleanup():
        s3 = boto3.client(
            "s3",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            endpoint_url="http://minio.docker.localhost",
        )
        buckets = s3.list_buckets()
        buckets = [x["Name"] for x in buckets["Buckets"]]
        for bucket in buckets:
            if "test-" in bucket:
                objects = s3.list_objects_v2(Bucket=bucket)
                if objects["KeyCount"]:
                    objects = [{"Key": x["Key"]} for x in objects.get("Contents", [])]
                    s3.delete_objects(Bucket=bucket, Delete=dict(Objects=objects))
                s3.delete_bucket(Bucket=bucket)
        # https://github.com/boto/botocore/pull/1810
        s3._endpoint.http_session._manager.clear()
