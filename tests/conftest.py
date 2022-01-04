import logging
import os
import secrets
from pathlib import Path

import boto3
import pytest
import redis as pyredis
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

from iris.agent.settings import AgentSettings
from iris.api.authentication import current_verified_user
from iris.api.dependencies import get_session, get_settings
from iris.api.main import app
from iris.api.settings import APISettings
from iris.commons.clickhouse import ClickHouse
from iris.commons.models.base import Base
from iris.commons.redis import AgentRedis, Redis
from iris.commons.settings import CommonSettings
from iris.commons.storage import Storage
from iris.worker import WorkerSettings

pytest.register_assert_rewrite("tests.assertions")
pytest_plugins = ["tests.fixtures.models", "tests.fixtures.storage"]

# Iris tests requires XXXX
# Each test runs on a dedicated namespace (redis namespace, bucket prefixes, SQLite database, CH databse, ...)
# To keep the result of each test runs, set IRIS_TEST_CLEANUP=0


@pytest.fixture
def settings():
    namespace = secrets.token_hex(nbytes=4)
    print(f"@{namespace}", end=" ")
    return CommonSettings(
        AWS_PUBLIC_RESOURCES=["arn:aws:s3:::test-public-exports/*"],
        AWS_S3_ARCHIVE_BUCKET_PREFIX=f"archive-test-{namespace}-",
        AWS_S3_TARGETS_BUCKET_PREFIX=f"targets-test-{namespace}-",
        AWS_TIMEOUT=0,
        CLICKHOUSE_URL=f"http://iris:iris@clickhouse.docker.localhost/?database=iris_test",
        CLICKHOUSE_TIMEOUT=0,
        REDIS_NAMESPACE=f"iris-test-{namespace}",
        # Redis has 16 databases by default, we use the last one for testing.
        REDIS_URL="redis://default:redispass@redis.docker.localhost?db=15",
        REDIS_TIMEOUT=0,
        SQLALCHEMY_DATABASE_URL=f"sqlite:///iris-test-{namespace}.sqlite3",
    )


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
        **settings.dict(), WORKER_RESULTS_DIR_PATH=tmp_path / "worker_results"
    )


@pytest.fixture
def logger():
    return logging.getLogger(__name__)


@pytest.fixture
def clickhouse(settings, logger):
    return ClickHouse(settings, logger)


@pytest.fixture
def engine(settings):
    # See https://sqlmodel.tiangolo.com/tutorial/fastapi/tests/ for more information.
    engine = create_engine(
        settings.SQLALCHEMY_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
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


@pytest.fixture(autouse=True, scope="session")
def cleanup():
    yield
    if os.environ.get("IRIS_TEST_CLEANUP", "") == "0":
        return
    # SQLite cleanup
    for file in Path().glob("iris-test-*.sqlite3"):
        file.unlink()
    # S3 cleanup
    # TODO: Refactor with base-settings?
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
    # Redis cleanup
    redis = pyredis.from_url("redis://default:redispass@redis.docker.localhost?db=15")
    redis.flushdb()


# TODO: Type fixtures
@pytest.fixture
def make_agent_redis(redis, settings):
    def _make_agent_redis(agent_uuid):
        return AgentRedis(redis.client, redis.settings, redis.logger, agent_uuid)

    return _make_agent_redis


@pytest.fixture
def make_client(engine, settings):
    def _make_client(user):
        # We need to override `get_session` since we use an in-memory engine for testing
        # and by default the API would instantiate a separate engine.
        # Note that we use the same *engine* but a different *session* in order to avoid
        # SQLAlchemy caching and be able to test against changes in object properties.
        def get_session_override():
            with Session(engine) as session:
                yield session

        # NOTE: We override `current_verified_user`, so the API will
        # always see a verified user, regardless of `is_verified`.
        app.dependency_overrides[current_verified_user] = lambda: user
        app.dependency_overrides[get_session] = get_session_override
        app.dependency_overrides[get_settings] = lambda: APISettings(**settings.dict())
        return TestClient(app)

    yield _make_client
    app.dependency_overrides.clear()
