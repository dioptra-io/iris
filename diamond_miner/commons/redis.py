import aioredis
import asyncio

from uuid import uuid4


class Redis(object):
    """Redis interface."""

    def __init__(self, uuid=None):
        self.uuid = uuid if uuid is not None else str(uuid4())

        self.redis = None
        self.info = None

    async def connect(self, host):
        """Connect to Redis instance."""
        self.redis = await aioredis.create_redis(host)
        await self.redis.client_setname(self.uuid)
        self.info = await self.whoami()

    async def whoami(self):
        """Get Redis own client information."""
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
        clients = await self.redis.client_list()
        clients = [c for c in clients if c.name and not c.name.startswith("controller")]
        states = await asyncio.gather(*[self.client_state(c.name) for c in clients])
        return [(c, s) for c, s in zip(clients, states)]

    async def get(self, *args, **kwargs):
        """Get a value from a key input."""
        return await self.redis.get(*args, **kwargs)

    async def set(self, *args, **kwargs):
        """Set a value from a key input."""
        return await self.redis.set(*args, **kwargs)

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

    # async def _reader(self, channel):
    #     while await channel.wait_message():
    #         return await channel.get_json()

    # async def subscribe(self, channel_name):
    #     """Subscribe to a channel and wait for a response"""
    #     channels = await self.redis.subscribe(channel_name)
    #     task = asyncio.create_task(self._reader(channels[0]))
    #     return task

    # async def unsubscribe(self, channel_name):
    #     await self.redis.unsubscribe(channel_name)
