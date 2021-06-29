import json
import logging
import ssl

import aioredis
import aioredis.pubsub
from aioredis.errors import ConnectionClosedError
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_delay,
    wait_exponential,
    wait_random,
)


class Redis(object):
    """Redis interface."""

    KEY_MEASUREMENT_STATE: str = "measurement_state"
    KEY_MEASUREMENT_STATS: str = "measurement_stats"

    KEY_AGENT_LISTEN: str = "agent_listen"
    KEY_AGENT_STATE: str = "agent_state"
    KEY_AGENT_PARAMETERS: str = "agent_parameters"

    def __init__(self, settings, logger=None):
        self._redis = None
        self.settings = settings
        self.logger = logger

    def fault_tolerant(func):
        """Exponential back-off strategy."""

        async def wrapper(*args, **kwargs):
            cls = args[0]
            settings, logger = cls.settings, cls.logger
            return await retry(
                stop=stop_after_delay(settings.REDIS_TIMEOUT),
                wait=wait_exponential(
                    multiplier=settings.REDIS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
                    min=settings.REDIS_TIMEOUT_EXPONENTIAL_MIN,
                    max=settings.REDIS_TIMEOUT_EXPONENTIAL_MAX,
                )
                + wait_random(
                    settings.REDIS_TIMEOUT_RANDOM_MIN,
                    settings.REDIS_TIMEOUT_RANDOM_MAX,
                ),
                before_sleep=(
                    before_sleep_log(logger, logging.ERROR) if logger else None
                ),
            )(func)(*args, **kwargs)

        return wrapper

    @fault_tolerant
    async def connect(self, host, password=None):
        """Connect to Redis instance."""
        ssl_context = ssl.SSLContext() if self.settings.REDIS_SSL else None
        self._redis = await aioredis.create_redis(host, ssl=ssl_context)
        if password:
            await self._redis.auth(password)

    @fault_tolerant
    async def get_agent_state(self, uuid):
        """Get agent state."""
        state = await self._redis.get(f"{self.KEY_AGENT_STATE}:{uuid}")
        if state is None:
            return "unknown"
        return state.decode("utf8")

    @fault_tolerant
    async def get_agent_parameters(self, uuid):
        """Get agent parameters."""
        parameters = await self._redis.get(f"{self.KEY_AGENT_PARAMETERS}:{uuid}")
        if parameters is None:
            return {}
        return json.loads(parameters)

    @fault_tolerant
    async def get_agents(self, state=True, parameters=True):
        """Get agents UUID along with their state."""
        agents_list = await self._redis.client_list()
        agents_list = [agent.name for agent in agents_list if agent.name]

        agents = []
        for agent_uuid in agents_list:
            agent = {"uuid": agent_uuid}
            if state:
                agent_state = await self.get_agent_state(agent_uuid)
                agent["state"] = agent_state
            if parameters:
                agent_parameters = await self.get_agent_parameters(agent_uuid)
                agent["parameters"] = agent_parameters

            agents.append(agent)
        return agents

    @fault_tolerant
    async def check_agent(self, uuid):
        """Check the conformity of an agent."""
        agents = await self.get_agents(state=False, parameters=False)
        agents = [agent["uuid"] for agent in agents]
        if uuid not in agents:
            return False
        agent_state = await self.get_agent_state(uuid)
        if agent_state == "unknown":
            return False
        agent_parameters = await self.get_agent_parameters(uuid)
        if not agent_parameters:
            return False
        return True

    @fault_tolerant
    async def get_measurement_state(self, uuid):
        """Get measurement state."""
        state = await self._redis.get(f"{self.KEY_MEASUREMENT_STATE}:{uuid}")
        if state is not None:
            return state.decode("utf8")

    @fault_tolerant
    async def set_measurement_state(self, uuid, state):
        """Set measurement state."""
        await self._redis.set(f"{self.KEY_MEASUREMENT_STATE}:{uuid}", state)

    @fault_tolerant
    async def delete_measurement_state(self, uuid):
        """Delete measurement state."""
        await self._redis.delete(f"{self.KEY_MEASUREMENT_STATE}:{uuid}")

    @fault_tolerant
    async def get_measurement_stats(self, measurement_uuid, agent_uuid):
        """Get measurement statistics."""
        state = await self._redis.get(
            f"{self.KEY_MEASUREMENT_STATS}:{measurement_uuid}:{agent_uuid}"
        )
        if state is not None:
            return json.loads(state.decode("utf8"))
        return {}

    @fault_tolerant
    async def set_measurement_stats(self, measurement_uuid, agent_uuid, stats):
        """Set measurement statistics."""
        await self._redis.set(
            f"{self.KEY_MEASUREMENT_STATS}:{measurement_uuid}:{agent_uuid}",
            json.dumps(stats),
        )

    @fault_tolerant
    async def delete_measurement_stats(self, measurement_uuid, agent_uuid):
        """Delete measurement statistics."""
        await self._redis.delete(
            f"{self.KEY_MEASUREMENT_STATS}:{measurement_uuid}:{agent_uuid}"
        )

    @fault_tolerant
    async def publish(self, channel, data):
        """Publish a message via into a channel."""
        await self._redis.publish_json(f"{self.KEY_AGENT_LISTEN}:{channel}", data)

    @fault_tolerant
    async def disconnect(self):
        """Close the connection."""
        self._redis.close()
        await self._redis.wait_closed()


class AgentRedis(Redis):
    """Redis interface for agents."""

    def __init__(self, uuid, settings, logger=None):
        self.uuid = uuid
        self.settings = settings
        self.logger = logger

    def fault_tolerant(func):
        """Exponential back-off strategy."""

        async def wrapper(*args, **kwargs):
            cls = args[0]
            settings, logger = cls.settings, cls.logger
            return await retry(
                stop=stop_after_delay(settings.REDIS_TIMEOUT),
                wait=wait_exponential(
                    multiplier=settings.REDIS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
                    min=settings.REDIS_TIMEOUT_EXPONENTIAL_MIN,
                    max=settings.REDIS_TIMEOUT_EXPONENTIAL_MAX,
                )
                + wait_random(
                    settings.REDIS_TIMEOUT_RANDOM_MIN,
                    settings.REDIS_TIMEOUT_RANDOM_MAX,
                ),
                before_sleep=(
                    before_sleep_log(logger, logging.ERROR) if logger else None
                ),
            )(func)(*args, **kwargs)

        return wrapper

    @fault_tolerant
    async def connect(self, host, password=None, register=True):
        """Connect to Redis instance."""
        await super().connect(host, password=password)
        if register:
            await self._redis.client_setname(self.uuid)

    async def test(self):
        """Test redis connection."""
        try:
            await self._redis.get(f"{self.KEY_AGENT_STATE}:{self.uuid}")
        except ConnectionClosedError:
            return False
        return True

    @fault_tolerant
    async def set_agent_state(self, state):
        """Set agent state."""
        await self._redis.set(f"{self.KEY_AGENT_STATE}:{self.uuid}", state)

    @fault_tolerant
    async def delete_agent_state(self):
        """Delete agent state."""
        await self._redis.delete(f"{self.KEY_AGENT_STATE}:{self.uuid}")

    @fault_tolerant
    async def set_agent_parameters(self, parameters):
        """Set agent parameters."""
        parameters = json.dumps(parameters)
        await self._redis.set(f"{self.KEY_AGENT_PARAMETERS}:{self.uuid}", parameters)

    @fault_tolerant
    async def delete_agent_parameters(self):
        """Delete agent state."""
        await self._redis.delete(f"{self.KEY_AGENT_PARAMETERS}:{self.uuid}")

    @fault_tolerant
    async def subscribe(self):
        """Subscribe to agent channels (all, specific) and wait for a response"""
        mpsc = aioredis.pubsub.Receiver()

        await self._redis.subscribe(
            mpsc.channel(f"{self.KEY_AGENT_LISTEN}:all"),
            mpsc.channel(f"{self.KEY_AGENT_LISTEN}:{self.uuid}"),
        )

        async for _, data in mpsc.iter():
            response = json.loads(data)
            break

        await self.unsubscribe()

        return response

    @fault_tolerant
    async def unsubscribe(self):
        """Unsubscribe to channels."""
        await self._redis.unsubscribe(
            f"{self.KEY_AGENT_LISTEN}:all", f"{self.KEY_AGENT_LISTEN}:{self.uuid}"
        )
