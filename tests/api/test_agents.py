def test_get_agents(client):
    class FakeRedis(object):
        async def get_agents(*args, **kwargs):
            return [
                {
                    "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
                    "state": "idle",
                    "parameters": {
                        "version": "0.1.0",
                        "hostname": "test",
                        "ipv4_address": "1.2.3.4",
                        "ipv6_address": "::1234",
                        "min_ttl": 1,
                        "max_probing_rate": 1000,
                    },
                }
            ]

    client.app.redis = FakeRedis()

    response = client.get("/api/agents")
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
                    "ipv4_address": "1.2.3.4",
                    "ipv6_address": "::1234",
                    "min_ttl": 1,
                    "max_probing_rate": 1000,
                },
            }
        ],
    }


def test_get_agent_by_uuid(client):
    class FakeRedis(object):
        async def get_agents(*args, **kwargs):
            return [{"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"}]

        async def get_agent_state(*args, **kwargs):
            return "idle"

        async def get_agent_parameters(*args, **kwargs):
            return {
                "version": "0.1.0",
                "hostname": "test",
                "ipv4_address": "1.2.3.4",
                "ipv6_address": "::1234",
                "min_ttl": 1,
                "max_probing_rate": 1000,
            }

    client.app.redis = FakeRedis()

    response = client.get("/api/agents/6f4ed428-8de6-460e-9e19-6e6173776552")
    assert response.json() == {
        "uuid": "6f4ed428-8de6-460e-9e19-6e6173776552",
        "state": "idle",
        "parameters": {
            "version": "0.1.0",
            "hostname": "test",
            "ipv4_address": "1.2.3.4",
            "ipv6_address": "::1234",
            "min_ttl": 1,
            "max_probing_rate": 1000,
        },
    }


def test_get_agent_by_uuid_not_found(client):
    class FakeRedis(object):
        async def get_agents(*args, **kwargs):
            return [{"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"}]

    client.app.redis = FakeRedis()

    response = client.get("/api/agents/6f4ed428-8de6-460e-9e19-6e6173776551")
    assert response.status_code == 404


def test_get_agent_by_uuid_duplicate(client):
    class FakeRedis(object):
        async def get_agents(*args, **kwargs):
            return [
                {"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"},
                {"uuid": "6f4ed428-8de6-460e-9e19-6e6173776552"},
            ]

    client.app.redis = FakeRedis()

    response = client.get("/api/agents/6f4ed428-8de6-460e-9e19-6e6173776552")
    assert response.status_code == 500
