import logging
from datetime import datetime
from ipaddress import IPv4Address, IPv6Address
from uuid import uuid4

import aioredis
import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient

from iris.api.authentication import current_verified_user
from iris.api.dependencies import get_database, get_redis
from iris.api.main import app
from iris.commons.database import Database
from iris.commons.redis import AgentRedis, Redis
from iris.commons.schemas.agents import Agent, AgentParameters, AgentState
from iris.commons.schemas.measurements import ProbingStatistics, Round
from iris.commons.schemas.users import User
from iris.commons.settings import CommonSettings


@pytest.fixture
def agent():
    return Agent(
        uuid=uuid4(),
        parameters=AgentParameters(
            version="0.1.0",
            hostname="localhost",
            ipv4_address=IPv4Address("127.0.0.1"),
            ipv6_address=IPv6Address("::1"),
            min_ttl=1,
            max_probing_rate=1000,
            agent_tags=["test"],
        ),
        state=AgentState.Idle,
    )


@pytest.fixture
def user():
    user = User(
        email="foo.bar@mail.com",
        is_active=True,
        is_verified=True,
        is_superuser=True,
        hashed_password="$2y$12$seiW.kzNc9NFRlpQpyeKie.PUJGhAtxn6oGPB.XfgnmTKx8Y9XCve",
        probing_enabled=True,
        probing_limit=10_000_000,
    )
    return user


@pytest.fixture
def statistics():
    return ProbingStatistics(
        round=Round(number=1, limit=10, offset=0),
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow(),
        filtered_low_ttl=0,
        filtered_high_ttl=0,
        filtered_prefix_excl=0,
        filtered_prefix_not_incl=0,
        probes_read=240,
        packets_sent=240,
        packets_failed=0,
        packets_received=72,
        packets_received_invalid=0,
        pcap_received=240,
        pcap_dropped=0,
        pcap_interface_dropped=0,
    )


@pytest.fixture(scope="function")
def common_settings():
    # The `function` scope ensures that the settings are reset before every test.
    return CommonSettings(
        AWS_TIMEOUT=0,
        DATABASE_URL="http://iris:iris@clickhouse.docker.localhost/?database=iris_test",
        DATABASE_TIMEOUT=0,
        REDIS_TIMEOUT=0,
        SQLALCHEMY_DATABASE_URL="sqlite://",
    )


@pytest.fixture(scope="function")
async def database(common_settings):
    database = Database(common_settings, logging.getLogger(__name__))
    return database


@pytest.fixture(scope="function")
async def redis_client(common_settings):
    # NOTE: When we were using fakeredis the store was reset between each calls.
    # We reproduce this behavior here.
    await aioredis.from_url(common_settings.REDIS_URL).flushall()
    return aioredis.from_url(common_settings.REDIS_URL, decode_responses=True)


@pytest.fixture(scope="function")
def redis(common_settings, redis_client):
    return Redis(redis_client, common_settings, logging.getLogger(__name__))


@pytest.fixture(scope="function")
def agent_redis(common_settings, redis_client, agent):
    return AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), agent.uuid
    )


@pytest.fixture(scope="function")
def api_client_factory(common_settings, redis_client):
    def api_client(override_user, klass=AsyncClient):
        async def override_get_redis():
            yield Redis(redis_client, common_settings, logging.getLogger(__name__))

        app.dependency_overrides = {
            get_database: lambda: Database(
                common_settings, logging.getLogger(__name__)
            ),
            get_redis: override_get_redis,
        }

        if override_user:
            app.dependency_overrides[current_verified_user] = lambda: override_user

        kwargs = dict(app=app, base_url="http://testserver")
        if klass == AsyncClient:
            kwargs["follow_redirects"] = True
        client = klass(**kwargs)
        return client

    return api_client


@pytest.fixture(scope="function")
def api_client(api_client_factory, user):
    return api_client_factory(override_user=user, klass=AsyncClient)


@pytest.fixture(scope="function")
def api_client_sync(api_client_factory, user):
    return api_client_factory(override_user=user, klass=TestClient)
