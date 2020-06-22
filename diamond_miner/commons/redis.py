import aioredis
import asyncio


class Redis(object):
    """Redis interface."""

    def __init__(self, uuid=None):
        self.uuid = uuid

        self.redis = None
        self.info = None

    async def connect(self, host, password=None):
        """Connect to Redis instance."""
        self.redis = await aioredis.create_redis(host)
        if password:
            await self.redis.auth(password)
        if self.uuid:
            await self.redis.client_setname(self.uuid)
            self.info = await self.whoami()

    async def whoami(self):
        """Get Redis own client information."""
        if not self.uuid:
            return
        clients = await self.redis.client_list()
        infos = [c for c in clients if c.name == self.uuid]
        if len(infos) != 1:
            raise ValueError("Redis UUID Collision")

        self.info = infos[0]
        return self.info

    async def client_state(self, uuid):
        """Get client state."""
        state = await self.redis.get(f"state:{uuid}")
        if state is None:
            return None
        return bool(int(state))

    async def agents_info(self):
        """Get client information."""
        agents = await self.redis.client_list()
        agents = [agent.name for agent in agents if agent.name]
        states = await asyncio.gather(*[self.client_state(agent) for agent in agents])
        return [(c, s) for c, s in zip(agents, states)]

    async def get(self, *args, **kwargs):
        """Get a value from a key input."""
        return await self.redis.get(*args, **kwargs)

    async def set(self, *args, **kwargs):
        """Set a value from a key input."""
        return await self.redis.set(*args, **kwargs)

    async def delete(self, *args, **kwargs):
        return await self.redis.delete(*args, **kwargs)

    async def register_measurement(self, measurement_uuid):
        """Register a measurement."""
        await self.redis.lpush("measurements", measurement_uuid)

    async def get_measurements(self):
        """Get all registered measurements."""
        return await self.redis.lrange("measurements", 0, -1)

    async def publish(self, channel_name, data):
        """Publish a message via into a channel."""
        await self.redis.publish_json(channel_name, data)

    async def subscribe(self, *args, **kwargs):
        """Subscribe to a channel and wait for a response"""
        channels = await self.redis.subscribe(*args, **kwargs)
        channel = channels[0]
        while await channel.wait_message():
            response = await channel.get_json()
            break
        await self.redis.unsubscribe(*args, **kwargs)
        return response

    async def close(self):
        """Close the connection."""
        self.redis.close()
        await self.redis.wait_closed()
