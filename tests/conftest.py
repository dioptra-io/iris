import pytest

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
        return [
            {
                "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                "state": "idle",
                "parameters": {
                    "version": "0.1.0",
                    "hostname": "test",
                    "ip_address": "1.2.3.4",
                    "probing_rate": 1000,
                    "buffer_sniffer_size": 500000,
                    "inf_born": 0,
                    "sup_born": 4294967295,
                    "ips_per_subnet": 6,
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
    app.dependency_overrides[authenticate] = override_authenticate
    app.redis = FakeRedis()
    return client
