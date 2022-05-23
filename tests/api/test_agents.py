from uuid import uuid4

from iris.commons.models.agent import Agent, AgentState
from iris.commons.models.pagination import Paginated
from tests.assertions import assert_response, assert_status_code
from tests.helpers import register_agent


def test_get_agents_probing_not_enabled(make_client, make_user):
    client = make_client(make_user(probing_enabled=False))
    assert_status_code(client.get("/agents"), 403)


async def test_get_agents_empty(make_client, make_user, make_agent_parameters):
    client = make_client(make_user(probing_enabled=True))
    assert_response(client.get("/agents"), Paginated[Agent](count=0, results=[]))


async def test_get_agents(make_client, make_user, make_agent_parameters, redis):
    client = make_client(make_user(probing_enabled=True))

    agents = [
        Agent(
            uuid=str(uuid4()),
            parameters=make_agent_parameters(),
            state=AgentState.Idle,
        )
    ]

    for agent in agents:
        await register_agent(redis, agent.uuid, agent.parameters, agent.state)

    # TODO: Add more agents and handle unordered comparisons.
    assert_response(
        client.get("/agents"), Paginated[Agent](count=len(agents), results=agents)
    )


async def test_get_agents_tag(make_client, make_user, make_agent_parameters, redis):
    client = make_client(make_user(probing_enabled=True))
    agents = [
        Agent(
            uuid=str(uuid4()),
            parameters=make_agent_parameters(),
            state=AgentState.Idle,
        ),
        Agent(
            uuid=str(uuid4()),
            parameters=make_agent_parameters(tags=["test-agent"]),
            state=AgentState.Idle,
        ),
    ]
    for agent in agents:
        await register_agent(redis, agent.uuid, agent.parameters, agent.state)
    assert_response(
        client.get("/agents", params=dict(tag="test-agent")),
        Paginated[Agent](count=1, results=agents[1:]),
    )


async def test_get_agent_probing_not_enabled(make_client, make_user):
    client = make_client(make_user(probing_enabled=False))
    assert_status_code(client.get(f"/agents/{uuid4()}"), 403)


async def test_get_agent_not_found(make_client, make_user):
    client = make_client(make_user(probing_enabled=True))
    assert_status_code(client.get(f"/agents/{uuid4()}"), 404)


async def test_get_agent(make_client, make_user, make_agent_parameters, redis):
    client = make_client(make_user(probing_enabled=True))
    agent = Agent(
        uuid=str(uuid4()),
        parameters=make_agent_parameters(),
        state=AgentState.Idle,
    )
    await register_agent(redis, agent.uuid, agent.parameters, agent.state)
    assert_response(client.get(f"/agents/{agent.uuid}"), agent)
