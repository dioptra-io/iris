"""Test of `agents` operation."""


def test_get_agents(client):
    """Test get agents."""

    class FakeRedis(object):
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
                        "pfring": False,
                    },
                }
            ]

    client.app.redis = FakeRedis()

    response = client.get("/v0/agents")
    assert response.json() == {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                "state": "idle",
                "parameters": {
                    "version": "0.1.0",
                    "hostname": "test",
                    "ip_address": "1.2.3.4",
                    "probing_rate": 1000,
                },
            }
        ],
    }


def test_get_agent_by_uuid(client):
    """Test get agent by uuid."""

    class FakeRedis(object):
        async def get_agents(*args, **kwargs):
            return [{"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"}]

        async def get_agent_state(*args, **kwargs):
            return "idle"

        async def get_agent_parameters(*args, **kwargs):
            return {
                "version": "0.1.0",
                "hostname": "test",
                "ip_address": "1.2.3.4",
                "probing_rate": 1000,
                "buffer_sniffer_size": 500000,
                "inf_born": 0,
                "sup_born": 4294967295,
                "ips_per_subnet": 6,
                "pfring": False,
            }

    client.app.redis = FakeRedis()

    response = client.get("/v0/agents/6f4ed428-8de6-460e-9e19-6e6173776552")
    assert response.json() == {
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
            "pfring": False,
        },
    }


def test_get_agent_by_uuid_not_found(client):
    """Get agent by uuid but not found."""

    class FakeRedis(object):
        async def get_agents(*args, **kwargs):
            return [{"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"}]

    client.app.redis = FakeRedis()

    response = client.get("/v0/agents/6f4ed428-8de6-460e-9e19-6e6173776551")
    assert response.status_code == 404


def test_get_agent_by_uuid_duplicate(client):
    """Get agent by uuid but duplicate."""

    class FakeRedis(object):
        async def get_agents(*args, **kwargs):
            return [
                {"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"},
                {"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"},
            ]

    client.app.redis = FakeRedis()

    response = client.get("/v0/agents/6f4ed428-8de6-460e-9e19-6e6173776552")
    assert response.status_code == 500
