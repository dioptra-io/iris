import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional
from uuid import UUID

import aioredis
import async_timeout

from iris.commons.schemas.private import MeasurementRoundRequest
from iris.commons.schemas.public import (
    Agent,
    AgentParameters,
    AgentState,
    MeasurementState,
    ProbingStatistics,
)
from iris.commons.settings import CommonSettings, fault_tolerant


def agent_heartbeat_key(uuid: Optional[UUID]) -> str:
    return f"agent_heartbeat:{uuid or '*'}"


def agent_listen_key(uuid: UUID) -> str:
    return f"agent_listen:{uuid}"


def agent_parameters_key(uuid: UUID) -> str:
    return f"agent_parameters:{uuid}"


def agent_state_key(uuid: UUID) -> str:
    return f"agent_state:{uuid}"


def measurement_state_key(uuid: UUID) -> str:
    return f"measurement_state:{uuid}"


def measurement_stats_key(measurement_uuid: UUID, agent_uuid: UUID) -> str:
    return f"measurement_stats:{measurement_uuid}:{agent_uuid}"


@dataclass(frozen=True)
class Redis:
    client: aioredis.Redis
    settings: CommonSettings
    logger: logging.Logger

    @fault_tolerant(CommonSettings.redis_retry)
    async def get_agent_state(self, uuid: UUID) -> AgentState:
        """Get agent state."""
        if state := await self.client.get(agent_state_key(uuid)):
            return AgentState(state)
        return AgentState.Unknown

    @fault_tolerant(CommonSettings.redis_retry)
    async def get_agent_parameters(self, uuid: UUID) -> Optional[AgentParameters]:
        """Get agent parameters."""
        if parameters := await self.client.get(agent_parameters_key(uuid)):
            return AgentParameters.parse_raw(parameters)
        return None

    @fault_tolerant(CommonSettings.redis_retry)
    async def get_agents(self) -> List[Agent]:
        """Get agents UUID along with their state."""
        # TODO: Use SCAN instead of KEYS for better scaling?
        alive = await self.client.keys(agent_heartbeat_key(None))
        uuids = [UUID(key.split(":")[1]) for key in alive]
        return [
            Agent(
                uuid=uuid,
                parameters=await self.get_agent_parameters(uuid),
                state=await self.get_agent_state(uuid),
            )
            for uuid in uuids
        ]

    async def get_agents_by_uuid(self) -> Dict[UUID, Agent]:
        agents = await self.get_agents()
        return {agent.uuid: agent for agent in agents}

    async def get_agent_by_uuid(self, uuid: UUID) -> Optional[Agent]:
        if await self.client.exists(agent_heartbeat_key(uuid)):
            return Agent(
                uuid=uuid,
                parameters=await self.get_agent_parameters(uuid),
                state=await self.get_agent_state(uuid),
            )
        return None

    @fault_tolerant(CommonSettings.redis_retry)
    async def check_agent(self, uuid: UUID) -> bool:
        """Check the conformity of an agent."""
        if not await self.client.exists(agent_heartbeat_key(uuid)):
            return False
        if not await self.get_agent_parameters(uuid):
            return False
        if await self.get_agent_state(uuid) == AgentState.Unknown:
            return False
        return True

    @fault_tolerant(CommonSettings.redis_retry)
    async def get_measurement_state(self, uuid: UUID) -> MeasurementState:
        """Get measurement state."""
        if state := await self.client.get(measurement_state_key(uuid)):
            return MeasurementState(state)
        return MeasurementState.Unknown

    @fault_tolerant(CommonSettings.redis_retry)
    async def set_measurement_state(self, uuid: UUID, state: MeasurementState):
        """Set measurement state."""
        await self.client.set(measurement_state_key(uuid), state)

    @fault_tolerant(CommonSettings.redis_retry)
    async def delete_measurement_state(self, uuid: UUID) -> None:
        """Delete measurement state."""
        await self.client.delete(measurement_state_key(uuid))

    @fault_tolerant(CommonSettings.redis_retry)
    async def get_measurement_stats(
        self, measurement_uuid: UUID, agent_uuid: UUID
    ) -> Optional[ProbingStatistics]:
        """Get measurement statistics."""
        if state := await self.client.get(
            measurement_stats_key(measurement_uuid, agent_uuid)
        ):
            return ProbingStatistics.parse_raw(state)
        return None

    @fault_tolerant(CommonSettings.redis_retry)
    async def set_measurement_stats(
        self, measurement_uuid: UUID, agent_uuid: UUID, stats: ProbingStatistics
    ) -> None:
        """Set measurement statistics."""
        await self.client.set(
            measurement_stats_key(measurement_uuid, agent_uuid), stats.json()
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def delete_measurement_stats(
        self, measurement_uuid: UUID, agent_uuid: UUID
    ) -> None:
        """Delete measurement statistics."""
        await self.client.delete(measurement_stats_key(measurement_uuid, agent_uuid))

    @fault_tolerant(CommonSettings.redis_retry)
    async def publish(self, uuid: UUID, request: MeasurementRoundRequest) -> None:
        """Publish a message via into a channel."""
        await self.client.publish(agent_listen_key(uuid), request.json())

    @fault_tolerant(CommonSettings.redis_retry)
    async def disconnect(self) -> None:
        """Close the connection."""
        await self.client.close()


@dataclass(frozen=True)
class AgentRedis(Redis):
    uuid: UUID

    @fault_tolerant(CommonSettings.redis_retry)
    async def register(self, ttl_seconds: int) -> None:
        await self.client.set(agent_heartbeat_key(self.uuid), "alive", ex=ttl_seconds)

    @fault_tolerant(CommonSettings.redis_retry)
    async def deregister(self) -> None:
        await self.client.delete(agent_heartbeat_key(self.uuid))

    @fault_tolerant(CommonSettings.redis_retry)
    async def set_agent_state(self, state: AgentState) -> None:
        """Set agent state."""
        await self.client.set(agent_state_key(self.uuid), state)

    @fault_tolerant(CommonSettings.redis_retry)
    async def delete_agent_state(self) -> None:
        """Delete agent state."""
        await self.client.delete(agent_state_key(self.uuid))

    @fault_tolerant(CommonSettings.redis_retry)
    async def set_agent_parameters(self, parameters: AgentParameters) -> None:
        """Set agent parameters."""
        await self.client.set(agent_parameters_key(self.uuid), parameters.json())

    @fault_tolerant(CommonSettings.redis_retry)
    async def delete_agent_parameters(self) -> None:
        """Delete agent state."""
        await self.client.delete(agent_parameters_key(self.uuid))

    @fault_tolerant(CommonSettings.redis_retry)
    async def subscribe(self) -> MeasurementRoundRequest:
        """Subscribe to agent channels (all, specific) and wait for a response"""
        psub = self.client.pubsub()
        async with psub as p:
            await p.subscribe(agent_listen_key(self.uuid))
            while True:
                try:
                    async with async_timeout.timeout(1.0):
                        message = await p.get_message(ignore_subscribe_messages=True)
                        if message:
                            response = MeasurementRoundRequest.parse_raw(
                                message["data"]
                            )
                            break
                        await asyncio.sleep(0.1)
                except asyncio.TimeoutError:
                    pass
            await p.unsubscribe()
        await psub.close()
        return response
