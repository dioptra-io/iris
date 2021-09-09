import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
from uuid import UUID

import aioredis
import async_timeout
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
    wait_random,
)

from iris.commons.schemas import private, public
from iris.commons.settings import CommonSettings


@dataclass(frozen=True)
class Redis:
    client: aioredis.Redis
    settings: CommonSettings
    logger: logging.Logger

    KEY_MEASUREMENT_STATE = "measurement_state"
    KEY_MEASUREMENT_STATS = "measurement_stats"

    KEY_AGENT_HEARTBEAT = "agent_heartbeat"
    KEY_AGENT_LISTEN = "agent_listen"
    KEY_AGENT_STATE = "agent_state"
    KEY_AGENT_PARAMETERS = "agent_parameters"

    def fault_tolerant(func):
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
    async def get_agent_state(self, uuid: UUID) -> public.AgentState:
        """Get agent state."""
        state = await self.client.get(f"{self.KEY_AGENT_STATE}:{uuid}")
        if state:
            return public.AgentState(state)
        return public.AgentState.Unknown

    @fault_tolerant
    async def get_agent_parameters(
        self, uuid: UUID
    ) -> Optional[public.AgentParameters]:
        """Get agent parameters."""
        parameters = await self.client.get(f"{self.KEY_AGENT_PARAMETERS}:{uuid}")
        if parameters:
            return public.AgentParameters.parse_raw(parameters)
        return None

    @fault_tolerant
    async def get_agents(self) -> List[public.Agent]:
        """Get agents UUID along with their state."""
        # TODO: Use SCAN instead of KEYS for better scaling?
        alive = await self.client.keys(f"{self.KEY_AGENT_HEARTBEAT}:*")
        uuids = [UUID(key.split(":")[1]) for key in alive]
        return [
            public.Agent(
                uuid=uuid,
                parameters=await self.get_agent_parameters(uuid),
                state=await self.get_agent_state(uuid),
            )
            for uuid in uuids
        ]

    async def get_agents_by_uuid(self) -> Dict[UUID, public.Agent]:
        agents = await self.get_agents()
        return {agent.uuid: agent for agent in agents}

    async def get_agent_by_uuid(self, uuid: UUID) -> Optional[public.Agent]:
        is_alive = await self.client.exists(f"{self.KEY_AGENT_HEARTBEAT}:{uuid}")
        if is_alive:
            return public.Agent(
                uuid=uuid,
                parameters=await self.get_agent_parameters(uuid),
                state=await self.get_agent_state(uuid),
            )
        return None

    @fault_tolerant
    async def check_agent(self, uuid: UUID) -> bool:
        """Check the conformity of an agent."""
        is_alive = await self.client.exists(f"{self.KEY_AGENT_HEARTBEAT}:{uuid}")
        if not is_alive:
            return False
        state = await self.get_agent_state(uuid)
        if state == public.AgentState.Unknown:
            return False
        parameters = await self.get_agent_parameters(uuid)
        if not parameters:
            return False
        return True

    @fault_tolerant
    async def get_measurement_state(self, uuid: UUID) -> public.MeasurementState:
        """Get measurement state."""
        state = await self.client.get(f"{self.KEY_MEASUREMENT_STATE}:{uuid}")
        if state:
            return public.MeasurementState(state)
        return public.MeasurementState.Unknown

    @fault_tolerant
    async def set_measurement_state(self, uuid: UUID, state: public.MeasurementState):
        """Set measurement state."""
        await self.client.set(f"{self.KEY_MEASUREMENT_STATE}:{uuid}", state)

    @fault_tolerant
    async def delete_measurement_state(self, uuid: UUID) -> None:
        """Delete measurement state."""
        await self.client.delete(f"{self.KEY_MEASUREMENT_STATE}:{uuid}")

    @fault_tolerant
    async def get_measurement_stats(
        self, measurement_uuid: UUID, agent_uuid: UUID
    ) -> Dict:
        """Get measurement statistics."""
        state = await self.client.get(
            f"{self.KEY_MEASUREMENT_STATS}:{measurement_uuid}:{agent_uuid}"
        )
        if state is not None:
            return json.loads(state)
        return {}

    @fault_tolerant
    async def set_measurement_stats(
        self, measurement_uuid: UUID, agent_uuid: UUID, stats: Dict
    ) -> None:
        """Set measurement statistics."""
        await self.client.set(
            f"{self.KEY_MEASUREMENT_STATS}:{measurement_uuid}:{agent_uuid}",
            json.dumps(stats),
        )

    @fault_tolerant
    async def delete_measurement_stats(
        self, measurement_uuid: UUID, agent_uuid: UUID
    ) -> None:
        """Delete measurement statistics."""
        await self.client.delete(
            f"{self.KEY_MEASUREMENT_STATS}:{measurement_uuid}:{agent_uuid}"
        )

    @fault_tolerant
    async def publish(self, channel: str, data: Dict) -> None:
        """Publish a message via into a channel."""
        await self.client.publish(
            f"{self.KEY_AGENT_LISTEN}:{channel}", json.dumps(data)
        )

    @fault_tolerant
    async def disconnect(self) -> None:
        """Close the connection."""
        await self.client.close()


@dataclass(frozen=True)
class AgentRedis(Redis):
    uuid: UUID

    def fault_tolerant(func):
        async def wrapper(*args, **kwargs):
            cls = args[0]
            settings, logger = cls.settings, cls.logger
            return await retry(
                stop=stop_after_delay(settings.REDIS_TIMEOUT),
                wait=wait_fixed(5)
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
    async def register(self, ttl_seconds: int) -> None:
        await self.client.set(
            f"{self.KEY_AGENT_HEARTBEAT}:{self.uuid}", "alive", ex=ttl_seconds
        )

    @fault_tolerant
    async def deregister(self) -> None:
        await self.client.delete(f"{self.KEY_AGENT_HEARTBEAT}:{self.uuid}")

    @fault_tolerant
    async def set_agent_state(self, state: public.AgentState) -> None:
        """Set agent state."""
        await self.client.set(f"{self.KEY_AGENT_STATE}:{self.uuid}", state)

    @fault_tolerant
    async def delete_agent_state(self) -> None:
        """Delete agent state."""
        await self.client.delete(f"{self.KEY_AGENT_STATE}:{self.uuid}")

    @fault_tolerant
    async def set_agent_parameters(self, parameters: public.AgentParameters) -> None:
        """Set agent parameters."""
        await self.client.set(
            f"{self.KEY_AGENT_PARAMETERS}:{self.uuid}", parameters.json()
        )

    @fault_tolerant
    async def delete_agent_parameters(self) -> None:
        """Delete agent state."""
        await self.client.delete(f"{self.KEY_AGENT_PARAMETERS}:{self.uuid}")

    @fault_tolerant
    async def subscribe(self) -> private.MeasurementRoundRequest:
        """Subscribe to agent channels (all, specific) and wait for a response"""
        psub = self.client.pubsub()
        async with psub as p:
            await p.subscribe(f"{self.KEY_AGENT_LISTEN}:all")
            await p.subscribe(f"{self.KEY_AGENT_LISTEN}:{self.uuid}")
            while True:
                try:
                    async with async_timeout.timeout(1.0):
                        message = await p.get_message(ignore_subscribe_messages=True)
                        if message:
                            response = private.MeasurementRoundRequest.parse_raw(
                                json.loads(message)
                            )
                            break
                        await asyncio.sleep(0.1)
                except asyncio.TimeoutError:
                    pass
            await p.unsubscribe()
        await psub.close()
        return response

    # TODO: Is this still necessary with aioredis 2.0.0?
    # @fault_tolerant
    # async def unsubscribe(self) -> None:
    #     """Unsubscribe from channels."""
    #     await self.client.unsubscribe(
    #         f"{self.KEY_AGENT_LISTEN}:all", f"{self.KEY_AGENT_LISTEN}:{self.uuid}"
    #     )
