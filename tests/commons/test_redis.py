"""Test of `Redis` commons classes."""

import aioredis
import pytest

from iris.commons.redis import Redis, AgentRedis


class FakeRedisConnection(object):
    def __init__(self, *args, **kwargs):
        self.methods = {}

    def assign(self, name, func):
        self.methods[name] = func

    def __getattr__(self, name):
        return self.methods[name]


def fake(value):
    async def wrapper(*args, **kwargs):
        return value

    return wrapper


def test_redis_attributes():
    """Test Redis global attributes."""
    redis = Redis()

    assert redis.KEY_MEASUREMENTS == "measurements"
    assert redis.KEY_MEASUREMENT_STATE == "measurement_state"

    assert redis.KEY_AGENT_LISTEN == "agent_listen"
    assert redis.KEY_AGENT_STATE == "agent_state"
    assert redis.KEY_AGENT_PARAMETERS == "agent_parameters"


@pytest.mark.asyncio
async def test_redis_connect(monkeypatch):
    """Test of `Redis.connect()` method."""
    redis = Redis()

    async def fake_create_redis(*args, **kwargs):
        async def fake_method(*args, **kwargs):
            return None

        fake = FakeRedisConnection()
        fake.assign("auth", fake_method)
        return fake

    monkeypatch.setattr(aioredis, "create_redis", fake_create_redis)

    assert await redis.connect("test") is None
    assert await redis.connect("test", "password") is None


@pytest.mark.asyncio
async def test_redis_get_agent_state():
    """Test of `Redis.get_agent_state()` method."""
    redis = Redis()
    redis._redis = FakeRedisConnection()

    redis._redis.assign("get", fake(None))
    assert await redis.get_agent_state("test") == "unknown"

    redis._redis.assign("get", fake(b"idle"))
    assert await redis.get_agent_state("test") == "idle"


@pytest.mark.asyncio
async def test_redis_get_agent_parameters():
    """Test of `Redis.et_agent_parameters()` method."""
    redis = Redis()
    redis._redis = FakeRedisConnection()

    redis._redis.assign("get", fake(None))
    assert await redis.get_agent_parameters("test") is None

    redis._redis.assign("get", fake(b'{"test":0}'))
    assert await redis.get_agent_parameters("test") == {"test": 0}


@pytest.mark.asyncio
async def test_redis_get_agents():
    """Test of `Redis.get_agents()` method."""
    redis = Redis()

    class Agent(object):
        def __init__(self, name):
            self.name = name

    redis._redis = FakeRedisConnection()
    redis._redis.assign("client_list", fake([]))

    assert await redis.get_agents() == []

    redis._redis = FakeRedisConnection()
    redis._redis.assign("client_list", fake([Agent(""), Agent("test")]))
    redis.get_agent_state = fake("idle")
    redis.get_agent_parameters = fake({"test": 0})

    assert await redis.get_agents() == [
        {"uuid": "test", "state": "idle", "parameters": {"test": 0}}
    ]
    assert await redis.get_agents(state=False) == [
        {"uuid": "test", "parameters": {"test": 0}}
    ]
    assert await redis.get_agents(state=False, parameters=False) == [{"uuid": "test"}]


@pytest.mark.asyncio
async def test_redis_check_agent():
    """Test of `Redis.check_agent()` method."""
    redis = Redis()

    redis.get_agents = fake([])
    assert await redis.check_agent("test") is False

    redis.get_agents = fake([{"uuid": "test"}])
    redis.get_agent_state = fake("unknown")
    assert await redis.check_agent("test") is False

    redis.get_agents = fake([{"uuid": "test"}])
    redis.get_agent_state = fake("idle")
    redis.get_agent_parameters = fake(None)
    assert await redis.check_agent("test") is False

    redis.get_agents = fake([{"uuid": "test"}])
    redis.get_agent_state = fake("idle")
    redis.get_agent_parameters = fake({"test": 0})
    assert await redis.check_agent("test") is True


@pytest.mark.asyncio
async def test_redis_get_measurement_state():
    """Test of `Redis.get_measurement_state()` method."""
    redis = Redis()
    redis._redis = FakeRedisConnection()

    redis._redis.assign("get", fake(None))
    assert await redis.get_measurement_state("test") is None

    redis._redis.assign("get", fake(b"ongoing"))
    assert await redis.get_measurement_state(b"test") == "ongoing"


@pytest.mark.asyncio
async def test_redis_set_measurement_state():
    """Test of `Redis.set_measurement_state()` method."""
    redis = Redis()
    redis._redis = FakeRedisConnection()

    redis._redis.assign("set", fake(None))
    assert await redis.set_measurement_state("test", "ongoing") is None


@pytest.mark.asyncio
async def test_redis_delete_measurement_state():
    """Test of `Redis.delete_measurement_state()` method."""
    redis = Redis()
    redis._redis = FakeRedisConnection()

    redis._redis.assign("delete", fake(None))
    assert await redis.delete_measurement_state("test") is None


@pytest.mark.asyncio
async def test_redis_publish():
    """Test of `Redis.publish()` method."""
    redis = Redis()
    redis._redis = FakeRedisConnection()

    redis._redis.assign("publish_json", fake(None))
    assert await redis.publish("channel", {"test": 0}) is None


@pytest.mark.asyncio
async def test_redis_close():
    """Test of `Redis.close()` method."""
    redis = Redis()
    redis._redis = FakeRedisConnection()

    redis._redis.assign("close", lambda: None)
    redis._redis.assign("wait_closed", fake(None))
    assert await redis.close() is None


@pytest.mark.asyncio
async def test_agent_redis_connect(monkeypatch):
    """Test of `AgentRedis.connect()` method."""
    redis = AgentRedis("test")

    async def fake_create_redis(*args, **kwargs):
        async def fake_method(*args, **kwargs):
            return None

        fake = FakeRedisConnection()
        fake.assign("auth", fake_method)
        fake.assign("client_setname", fake_method)
        return fake

    monkeypatch.setattr(aioredis, "create_redis", fake_create_redis)

    assert await redis.connect("test") is None
    assert await redis.connect("test", "password") is None
    assert await redis.connect("test", "password", register=False) is None


@pytest.mark.asyncio
async def test_redis_agent_set_agent_state():
    """Test of `AgentRedis.set_agent_state()` method."""
    redis = AgentRedis("test")
    redis._redis = FakeRedisConnection()

    redis._redis.assign("set", fake(None))
    assert await redis.set_agent_state("ongoing") is None


@pytest.mark.asyncio
async def test_redis_agent_delete_agent_state():
    """Test of `AgentRedis.delete_agent_state()` method."""
    redis = AgentRedis("test")
    redis._redis = FakeRedisConnection()

    redis._redis.assign("delete", fake(None))
    assert await redis.delete_agent_state() is None


@pytest.mark.asyncio
async def test_redis_agent_set_agent_parameters():
    """Test of `AgentRedis.set_agent_parameters()` method."""
    redis = AgentRedis("test")
    redis._redis = FakeRedisConnection()

    redis._redis.assign("set", fake(None))
    assert await redis.set_agent_parameters("ongoing") is None


@pytest.mark.asyncio
async def test_redis_agent_delete_agent_parameters():
    """Test of `AgentRedis.delete_agent_parameters()` method."""
    redis = AgentRedis("test")
    redis._redis = FakeRedisConnection()

    redis._redis.assign("delete", fake(None))
    assert await redis.delete_agent_parameters() is None


@pytest.mark.asyncio
async def test_redis_agent_subscribe(monkeypatch):
    """Test of `AgentRedis.subscribe()` method."""
    redis = AgentRedis("test")
    redis._redis = FakeRedisConnection()

    class FakeReceiver(object):
        def channel(*args, **kwargs):
            return None

        async def iter(self):
            yield None, b'{"test": 0}'

    monkeypatch.setattr(aioredis.pubsub, "Receiver", FakeReceiver)

    redis._redis.assign("subscribe", fake(None))
    redis._redis.assign("unsubscribe", fake(None))
    assert await redis.subscribe() == {"test": 0}
