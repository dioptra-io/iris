import aioredis
import json

from aioredis.pubsub import Receiver


class Redis(object):
    """Redis interface."""

    KEY_MEASUREMENTS: str = "measurements"
    KEY_MEASUREMENT_STATE: str = "measurement_state"

    KEY_AGENT_LISTEN: str = "agent_listen"
    KEY_AGENT_STATE: str = "agent_state"
    KEY_AGENT_PARAMETERS: str = "agent_parameters"

    def __init__(self, uuid=None):
        self._redis = None

    async def connect(self, host, password=None, register=True):
        """Connect to Redis instance."""
        self._redis = await aioredis.create_redis(host)
        if password:
            await self._redis.auth(password)

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

    async def get_agent_state(self, uuid):
        """Get agent state."""
        state = await self._redis.get(f"{self.KEY_AGENT_STATE}:{uuid}")
        if state is None:
            return "unknown"
        return state.decode("utf8")

    async def get_agent_parameters(self, uuid):
        """Get agent parameters."""
        parameters = await self._redis.get(f"{self.KEY_AGENT_PARAMETERS}:{uuid}")
        if parameters is None:
            return None
        return json.loads(parameters)

    async def check_agent(self, uuid):
        agents = await self.get_agents(state=False, parameters=False)
        agents = [agent["uuid"] for agent in agents]
        if uuid not in agents:
            return False
        agent_state = await self.get_agent_state(uuid)
        if agent_state == "unknown":
            return False
        agent_parameters = await self.get_agent_parameters(uuid)
        if agent_parameters is None:
            return False
        return True

    async def get_measurement_state(self, uuid):
        """Get measurement state."""
        state = await self._redis.get(f"{self.KEY_MEASUREMENT_STATE}:{uuid}")
        if state is not None:
            return state.decode("utf8")

    async def set_measurement_state(self, uuid, state):
        """Set measurement_parameters."""
        await self._redis.set(f"{self.KEY_MEASUREMENT_STATE}:{uuid}", state)

    async def delete_measurement_state(self, uuid):
        """Delete agent state."""
        await self._redis.delete(f"{self.KEY_MEASUREMENT_STATE}:{uuid}")

    async def publish(self, channel, data):
        """Publish a message via into a channel."""
        await self._redis.publish_json(f"{self.KEY_AGENT_LISTEN}:{channel}", data)

    async def close(self):
        """Close the connection."""
        self._redis.close()
        await self._redis.wait_closed()


class AgentRedis(Redis):
    """Redis interface for agents."""

    def __init__(self, uuid):
        super().__init__()
        self.uuid = uuid

    async def connect(self, host, password=None, register=True):
        """Connect to Redis instance."""
        await super().connect(host, password=password)
        if register:
            await self._redis.client_setname(self.uuid)

    async def set_agent_state(self, state):
        """Set agent state."""
        await self._redis.set(f"{self.KEY_AGENT_STATE}:{self.uuid}", state)

    async def delete_agent_state(self):
        """Delete agent state."""
        await self._redis.delete(f"{self.KEY_AGENT_STATE}:{self.uuid}")

    async def set_agent_parameters(self, parameters):
        """Set agent parameters."""
        parameters = json.dumps(parameters)
        await self._redis.set(f"{self.KEY_AGENT_PARAMETERS}:{self.uuid}", parameters)

    async def delete_agent_parameters(self):
        """Delete agent state."""
        await self._redis.delete(f"{self.KEY_AGENT_PARAMETERS}:{self.uuid}")

    async def subscribe(self):
        """Subscribe to agent channels (all, specific) and wait for a response"""
        mpsc = Receiver()

        await self._redis.subscribe(
            mpsc.channel(f"{self.KEY_AGENT_LISTEN}:all"),
            mpsc.channel(f"{self.KEY_AGENT_LISTEN}:{self.uuid}"),
        )

        async for _, data in mpsc.iter():
            response = json.loads(data)
            break

        await self._redis.unsubscribe(
            f"{self.KEY_AGENT_LISTEN}:all", f"{self.KEY_AGENT_LISTEN}:{self.uuid}"
        )

        return response
