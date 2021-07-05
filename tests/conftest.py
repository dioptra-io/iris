import logging
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from iris.api.main import app
from iris.api.security import get_current_active_user
from iris.api.settings import APISettings
from iris.commons.redis import Redis
from iris.commons.settings import CommonSettings

uuid_user = str(uuid4())


def override_get_current_active_user():
    return {
        "uuid": uuid_user,
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


class TestSettings(APISettings):
    pass


class FakeRedis(Redis):
    def __init__(*args, **kwargs):
        pass

    def connect(*args, **kwargs):
        pass

    async def get_agents(*args, **kwargs):
        return [
            {
                "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                "state": "idle",
                "parameters": {
                    "version": "0.1.0",
                    "hostname": "test",
                    "ip_address": "1.2.3.4",
                    "min_ttl": 1,
                    "max_probing_rate": 1000,
                    "agent_tags": ["test"],
                },
            }
        ]

    async def get_agent_state(*args, **kwargs):
        return "idle"

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


@pytest.fixture
def client():
    client = TestClient(app)
    app.dependency_overrides[get_current_active_user] = override_get_current_active_user

    app.redis = FakeRedis()
    app.logger = logging.getLogger("test")
    app.settings = TestSettings()
    return client


@pytest.fixture(scope="function")
def common_settings():
    # The `function` scope ensures that the settings are reset before every test.
    return CommonSettings(
        DATABASE_HOST="localhost", DATABASE_NAME="iris_test", DATABASE_TIMEOUT=0
    )
