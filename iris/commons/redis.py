import asyncio
import random
from dataclasses import dataclass
from logging import LoggerAdapter
from typing import Dict, List, Optional

import aioredis

from iris.commons.models import (
    Agent,
    AgentParameters,
    AgentState,
    MeasurementRoundRequest,
    ProbingStatistics,
)
from iris.commons.settings import CommonSettings, fault_tolerant


def agent_heartbeat_key(uuid: Optional[str]) -> str:
    return f"agent_heartbeat:{uuid or '*'}"


def agent_parameters_key(uuid: str) -> str:
    return f"agent_parameters:{uuid}"


def agent_state_key(uuid: str) -> str:
    return f"agent_state:{uuid}"


def agent_queue_key(uuid: str) -> str:
    return f"agent_queue:{uuid}"


def measurement_stats_key(measurement_uuid: str, agent_uuid: str) -> str:
    return f"measurement_stats:{measurement_uuid}:{agent_uuid}"


@dataclass(frozen=True)
class Redis:
    client: aioredis.Redis
    settings: CommonSettings
    logger: LoggerAdapter

    @property
    def ns(self) -> str:
        return self.settings.REDIS_NAMESPACE

    @fault_tolerant
    async def delete(self, *names: str) -> None:
        names_ = [f"{self.ns}:{name}" for name in names]
        await self.client.delete(*names_)

    @fault_tolerant
    async def exists(self, *names: str) -> int:
        names_ = [f"{self.ns}:{name}" for name in names]
        count: int = await self.client.exists(*names_)
        return count

    @fault_tolerant
    async def get(self, name: str) -> str:
        value: str = await self.client.get(f"{self.ns}:{name}")
        return value

    @fault_tolerant
    async def hdel(self, name: str, *keys: str) -> None:
        await self.client.hdel(f"{self.ns}:{name}", *keys)

    @fault_tolerant
    async def hget(self, name: str, key: str) -> Optional[str]:
        return await self.client.hget(f"{self.ns}:{name}", key)

    @fault_tolerant
    async def hkeys(self, name: str) -> List[str]:
        return await self.client.hkeys(f"{self.ns}:{name}")

    @fault_tolerant
    async def hset(self, name: str, key: str, value: str) -> None:
        await self.client.hset(f"{self.ns}:{name}", key, value)

    @fault_tolerant
    async def keys(self, pattern: str) -> List[str]:
        keys: List[str] = await self.client.keys(f"{self.ns}:{pattern}")
        return keys

    @fault_tolerant
    async def set(self, name: str, value: str, **kwargs) -> None:
        await self.client.set(f"{self.ns}:{name}", value, **kwargs)

    async def register_agent(self, uuid: str, ttl_seconds: int) -> None:
        self.logger.info("Registering agent for %s seconds", ttl_seconds)
        await self.set(agent_heartbeat_key(uuid), "alive", ex=ttl_seconds)

    async def unregister_agent(self, uuid: str) -> None:
        self.logger.info("Unregistering agent")
        await self.delete(agent_heartbeat_key(uuid))

    async def get_agent_state(self, uuid: str) -> AgentState:
        if v := await self.get(agent_state_key(uuid)):
            return AgentState(v)
        return AgentState.Unknown

    async def set_agent_state(self, uuid: str, state: AgentState) -> None:
        self.logger.info("Setting agent state to %s", state)
        await self.set(agent_state_key(uuid), state.value)

    async def delete_agent_state(self, uuid: str) -> None:
        self.logger.info("Deleting agent state")
        await self.delete(agent_state_key(uuid))

    async def get_agent_parameters(self, uuid: str) -> Optional[AgentParameters]:
        if v := await self.get(agent_parameters_key(uuid)):
            return AgentParameters.parse_raw(v)
        return None

    async def set_agent_parameters(
        self, uuid: str, parameters: AgentParameters
    ) -> None:
        self.logger.info("Setting agent parameters")
        await self.set(agent_parameters_key(uuid), parameters.json())

    async def delete_agent_parameters(self, uuid: str) -> None:
        self.logger.info("Deleting agent parameters")
        await self.delete(agent_parameters_key(uuid))

    async def get_agents(self) -> List[Agent]:
        # TODO: Use SCAN instead of KEYS for better scaling?
        alive = await self.keys(agent_heartbeat_key(None))
        uuids = [str(key.split(":")[2]) for key in alive]
        return [
            Agent(
                uuid=uuid,
                parameters=await self.get_agent_parameters(uuid),
                state=await self.get_agent_state(uuid),
            )
            for uuid in uuids
        ]

    async def get_agents_by_uuid(self) -> Dict[str, Agent]:
        agents = await self.get_agents()
        return {agent.uuid: agent for agent in agents}

    async def get_agent_by_uuid(self, uuid: str) -> Optional[Agent]:
        if await self.exists(agent_heartbeat_key(uuid)):
            return Agent(
                uuid=uuid,
                parameters=await self.get_agent_parameters(uuid),
                state=await self.get_agent_state(uuid),
            )
        return None

    @fault_tolerant
    async def check_agent(self, uuid: str) -> bool:
        if not await self.exists(agent_heartbeat_key(uuid)):
            return False
        if not await self.get_agent_parameters(uuid):
            return False
        if await self.get_agent_state(uuid) == AgentState.Unknown:
            return False
        return True

    async def get_measurement_stats(
        self, measurement_uuid: str, agent_uuid: str
    ) -> Optional[ProbingStatistics]:
        if state := await self.get(measurement_stats_key(measurement_uuid, agent_uuid)):
            return ProbingStatistics.parse_raw(state)
        return None

    async def set_measurement_stats(
        self, measurement_uuid: str, agent_uuid: str, stats: ProbingStatistics
    ) -> None:
        self.logger.info("Setting measurement statistics")
        await self.set(
            measurement_stats_key(measurement_uuid, agent_uuid), stats.json()
        )

    async def delete_measurement_stats(
        self, measurement_uuid: str, agent_uuid: str
    ) -> None:
        self.logger.info("Deleting measurement statistics")
        await self.delete(measurement_stats_key(measurement_uuid, agent_uuid))

    async def get_random_request(
        self, uuid: str, *, interval: float = 1.0
    ) -> MeasurementRoundRequest:
        """
        Return a random request from the queue.
        If the queue is empty, it will retry at the specified interval.
        """
        # TODO: Use HRANDFIELD when implement by aioredis.
        while True:
            if keys := await self.hkeys(agent_queue_key(uuid)):
                value = await self.hget(agent_queue_key(uuid), random.choice(keys))
                return MeasurementRoundRequest.parse_raw(value)
            await asyncio.sleep(interval)

    async def get_request(
        self, measurement_uuid: str, agent_uuid: str
    ) -> Optional[MeasurementRoundRequest]:
        """Return the measurement request for the specified agent and measurement."""
        if value := await self.hget(agent_queue_key(agent_uuid), measurement_uuid):
            return MeasurementRoundRequest.parse_raw(value)
        return None

    async def set_request(self, uuid: str, request: MeasurementRoundRequest) -> None:
        """Set the measurement request for a specified agent and measurement."""
        await self.hset(agent_queue_key(uuid), request.measurement_uuid, request.json())

    async def delete_request(self, measurement_uuid: str, agent_uuid: str) -> None:
        """Delete the measurement request for a specified agent and measurement."""
        await self.hdel(agent_queue_key(agent_uuid), measurement_uuid)
