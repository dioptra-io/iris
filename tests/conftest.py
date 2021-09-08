import logging
import subprocess
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient

from iris.api.main import app
from iris.api.security import get_current_active_user
from iris.api.settings import APISettings
from iris.commons.redis import Redis
from iris.commons.schemas.public import Agent, AgentParameters, AgentState
from iris.commons.settings import CommonSettings

agent = Agent(
    uuid=uuid4(),
    parameters=AgentParameters(
        version="0.1.0",
        hostname="localhost",
        ipv4_address="127.0.0.1",
        ipv6_address="::1",
        min_ttl=1,
        max_probing_rate=1000,
        agent_tags=["test"],
    ),
    state=AgentState.Idle,
)


@pytest.fixture
def fake_agent():
    return agent


class FakeRedis(Redis):
    def __init__(*args, **kwargs):
        pass

    def connect(*args, **kwargs):
        pass

    async def get_agents(*args, **kwargs):
        return [agent]

    async def get_agent_state(*args, **kwargs):
        return agent.state

    async def get_agent_parameters(*args, **kwargs):
        return {}

    async def check_agent(*args, **kwargs):
        return True

    async def get_measurement_state(*args, **kwargs):
        return None

    async def set_measurement_state(*args, **kwargs):
        pass

    async def delete_measurement_state(*args, **kwargs):
        pass

    async def publish(*args, **kwargs):
        pass

    async def disconnect(*args, **kwargs):
        pass


class TestSettings(APISettings):
    pass


def override_get_current_active_user():
    return {
        "uuid": str(uuid4()),
        "username": "test",
        "email": "test@test",
        "hashed_password": (
            "$2y$12$seiW.kzNc9NFRlpQpyeKie.PUJGhAtxn6oGPB.XfgnmTKx8Y9XCve"
        ),
        "is_active": True,
        "is_admin": True,
        "quota": 1000,
        "register_date": "date",
        "ripe_account": None,
        "ripe_key": None,
    }


@pytest.fixture(scope="session")
def s3_server():
    # We cannot use moto decorators directly since they are synchronous.
    # Instead we launch moto_server in a separate process, this is similar
    # to what is done by aiobotocore and aioboto3 for testing.
    p = subprocess.Popen(
        ["moto_server", "--host", "127.0.0.1", "--port", "3000", "s3"],
        stderr=subprocess.PIPE,
    )
    p.stderr.readline()  # Wait for moto_server to start
    yield "http://127.0.0.1:3000"
    p.terminate()


@pytest.fixture(scope="function")
def common_settings(s3_server):
    # The `function` scope ensures that the settings are reset before every test.
    return CommonSettings(
        AWS_S3_HOST=s3_server,
        AWS_TIMEOUT=0,
        DATABASE_HOST="localhost",
        DATABASE_NAME="iris_test",
        DATABASE_TIMEOUT=0,
        REDIS_TIMEOUT=0,
        REDIS_URL="redis://default:redispass@localhost",
    )


@pytest.fixture(scope="function")
async def redis_client(common_settings):
    return await common_settings.redis_client()


# Async API client with a real Redis client
@pytest.fixture(scope="function")
def api_client(common_settings, redis_client):
    app.dependency_overrides[get_current_active_user] = override_get_current_active_user
    app.logger = logging.getLogger("test")
    app.redis = Redis(redis_client, common_settings, app.logger)
    app.settings = TestSettings()
    client = AsyncClient(app=app, base_url="http://test")
    return client


# Sync API client with a fake Redis client
@pytest.fixture(scope="function")
def api_client_sync(common_settings, redis_client):
    app.dependency_overrides[get_current_active_user] = override_get_current_active_user
    app.logger = logging.getLogger("test")
    app.redis = FakeRedis()
    app.settings = TestSettings()
    client = TestClient(app)
    return client
