import logging
from uuid import uuid4

import pytest

from iris.commons.redis import AgentRedis


@pytest.mark.asyncio
async def test_get_agents(api_client, common_settings, fake_agent, redis_client):
    redis = AgentRedis(
        redis_client, common_settings, logging.getLogger(__name__), fake_agent.uuid
    )
    await redis.register()
    await redis.set_agent_parameters(fake_agent.parameters)
    await redis.set_agent_state(fake_agent.state)

    formatted = {
        "uuid": str(fake_agent.uuid),
        "state": "idle",
        "parameters": {
            "version": "0.1.0",
            "hostname": "localhost",
            "ipv4_address": "127.0.0.1",
            "ipv6_address": "::1",
            "min_ttl": 1,
            "max_probing_rate": 1000,
            "agent_tags": ["test"],
        },
    }

    async with api_client as c:
        response = await c.get("/api/agents")
        assert response.json() == {
            "count": 1,
            "next": None,
            "previous": None,
            "results": [formatted],
        }

        response = await c.get(f"/api/agents/{fake_agent.uuid}")
        assert response.json() == formatted

        response = await c.get(f"/api/agents/{uuid4()}")
        assert response.status_code == 404


# def test_get_agent_by_uuid_duplicate(client):
#     class FakeRedis(object):
#         async def get_agents(*args, **kwargs):
#             return [
#                 {"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"},
#                 {"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"},
#             ]
#
#     client.app.redis = FakeRedis()
#
#     response = client.get("/api/agents/6f4ed428-8de6-460e-9e19-6e6173776552")
#     assert response.status_code == 500
