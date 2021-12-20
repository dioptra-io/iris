from uuid import uuid4

import pytest

from iris.commons.schemas.agents import Agent
from iris.commons.schemas.paging import Paginated


@pytest.mark.asyncio
async def test_get_agents(api_client, agent_redis, agent):
    await agent_redis.register(5)
    await agent_redis.set_agent_parameters(agent.parameters)
    await agent_redis.set_agent_state(agent.state)

    async with api_client as c:
        response = await c.get("/agents")
        assert Paginated[Agent](**response.json()) == Paginated(
            count=1, results=[agent]
        )

        response = await c.get(f"/agents/{agent.uuid}")
        assert Agent(**response.json()) == agent

        response = await c.get(f"/agents/{uuid4()}")
        assert response.status_code == 404
