import pytest
import uuid

from fastapi.testclient import TestClient
from iris.api.main import app
from iris.api.security import authenticate
from iris.commons.redis import Redis


def override_authenticate():
    return "test"


class FakeRedis(Redis):
    def connect(*args, **kwargs):
        pass

    async def get_agents(*args, **kwargs):
        return [{"uuid": uuid.uuid4(), "state": "idle", "parameters": {}}]

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

    async def close(*args, **kwargs):
        pass


@pytest.fixture
def client():
    client = TestClient(app)
    app.dependency_overrides[authenticate] = override_authenticate
    app.redis = FakeRedis()
    return client
