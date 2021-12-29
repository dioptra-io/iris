import asyncio
from dataclasses import dataclass
from logging import LoggerAdapter
from typing import Dict, List, Optional

import aioredis
import async_timeout

from iris.commons.models.agent import Agent, AgentParameters, AgentState
from iris.commons.models.diamond_miner import ProbingStatistics
from iris.commons.models.measurement_agent import MeasurementAgentState
from iris.commons.models.measurement_round_request import MeasurementRoundRequest
from iris.commons.settings import CommonSettings, fault_tolerant


def agent_heartbeat_key(namespace: str, uuid: Optional[str]) -> str:
    return f"{namespace}:agent_heartbeat:{uuid or '*'}"


def agent_listen_key(namespace: str, uuid: str) -> str:
    return f"{namespace}:agent_listen:{uuid}"


def agent_parameters_key(namespace: str, uuid: str) -> str:
    return f"{namespace}:agent_parameters:{uuid}"


def agent_state_key(namespace: str, uuid: str) -> str:
    return f"{namespace}:agent_state:{uuid}"


def measurement_state_key(namespace: str, uuid: str) -> str:
    return f"{namespace}:measurement_state:{uuid}"


def measurement_stats_key(
    namespace: str, measurement_uuid: str, agent_uuid: str
) -> str:
    return f"{namespace}:measurement_stats:{measurement_uuid}:{agent_uuid}"


def measurement_agent_cancel_key(
    namespace: str, measurement_uuid: str, agent_uuid: str
) -> str:
    return f"{namespace}:measurement_agent_cancel:{measurement_uuid}:{agent_uuid}"


@dataclass(frozen=True)
class Redis:
    client: aioredis.Redis
    settings: CommonSettings
    logger: LoggerAdapter

    @fault_tolerant(CommonSettings.redis_retry)
    async def get_agent_state(self, uuid: str) -> AgentState:
        """Get agent state."""
        if state := await self.client.get(
            agent_state_key(self.settings.REDIS_NAMESPACE, uuid)
        ):
            return AgentState(state)
        return AgentState.Unknown

    @fault_tolerant(CommonSettings.redis_retry)
    async def get_agent_parameters(self, uuid: str) -> Optional[AgentParameters]:
        """Get agent parameters."""
        if parameters := await self.client.get(
            agent_parameters_key(self.settings.REDIS_NAMESPACE, uuid)
        ):
            return AgentParameters.parse_raw(parameters)
        return None

    @fault_tolerant(CommonSettings.redis_retry)
    async def get_agents(self) -> List[Agent]:
        """Get agents str along with their state."""
        # TODO: Use SCAN instead of KEYS for better scaling?
        alive = await self.client.keys(
            agent_heartbeat_key(self.settings.REDIS_NAMESPACE, None)
        )
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
        if await self.client.exists(
            agent_heartbeat_key(self.settings.REDIS_NAMESPACE, uuid)
        ):
            return Agent(
                uuid=uuid,
                parameters=await self.get_agent_parameters(uuid),
                state=await self.get_agent_state(uuid),
            )
        return None

    @fault_tolerant(CommonSettings.redis_retry)
    async def check_agent(self, uuid: str) -> bool:
        """Check the conformity of an agent."""
        if not await self.client.exists(
            agent_heartbeat_key(self.settings.REDIS_NAMESPACE, uuid)
        ):
            return False
        if not await self.get_agent_parameters(uuid):
            return False
        if await self.get_agent_state(uuid) == AgentState.Unknown:
            return False
        return True

    @fault_tolerant(CommonSettings.redis_retry)
    async def cancel_measurement_agent(
        self, measurement_uuid: str, agent_uuid: str
    ) -> None:
        await self.client.set(
            measurement_agent_cancel_key(
                self.settings.REDIS_NAMESPACE, measurement_uuid, agent_uuid
            ),
            1,
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def get_measurement_stats(
        self, measurement_uuid: str, agent_uuid: str
    ) -> Optional[ProbingStatistics]:
        """Get measurement statistics."""
        if state := await self.client.get(
            measurement_stats_key(
                self.settings.REDIS_NAMESPACE, measurement_uuid, agent_uuid
            )
        ):
            return ProbingStatistics.parse_raw(state)
        return None

    @fault_tolerant(CommonSettings.redis_retry)
    async def set_measurement_stats(
        self, measurement_uuid: str, agent_uuid: str, stats: ProbingStatistics
    ) -> None:
        """Set measurement statistics."""
        await self.client.set(
            measurement_stats_key(
                self.settings.REDIS_NAMESPACE, measurement_uuid, agent_uuid
            ),
            stats.json(),
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def delete_measurement_stats(
        self, measurement_uuid: str, agent_uuid: str
    ) -> None:
        """Delete measurement statistics."""
        await self.client.delete(
            measurement_stats_key(
                self.settings.REDIS_NAMESPACE, measurement_uuid, agent_uuid
            )
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def publish(self, uuid: str, request: MeasurementRoundRequest) -> None:
        """Publish a message into a channel."""
        self.logger.info("Publishing next measurement round request")
        await self.client.publish(
            agent_listen_key(self.settings.REDIS_NAMESPACE, uuid), request.json()
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def disconnect(self) -> None:
        """Close the connection."""
        await self.client.close()


@dataclass(frozen=True)
class AgentRedis(Redis):
    uuid: str

    @fault_tolerant(CommonSettings.redis_retry)
    async def register(self, ttl_seconds: int) -> None:
        await self.client.set(
            agent_heartbeat_key(self.settings.REDIS_NAMESPACE, self.uuid),
            "alive",
            ex=ttl_seconds,
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def deregister(self) -> None:
        await self.client.delete(
            agent_heartbeat_key(self.settings.REDIS_NAMESPACE, self.uuid)
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def set_agent_state(self, state: AgentState) -> None:
        """Set agent state."""
        await self.client.set(
            agent_state_key(self.settings.REDIS_NAMESPACE, self.uuid), state.value
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def delete_agent_state(self) -> None:
        """Delete agent state."""
        await self.client.delete(
            agent_state_key(self.settings.REDIS_NAMESPACE, self.uuid)
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def set_agent_parameters(self, parameters: AgentParameters) -> None:
        """Set agent parameters."""
        await self.client.set(
            agent_parameters_key(self.settings.REDIS_NAMESPACE, self.uuid),
            parameters.json(),
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def delete_agent_parameters(self) -> None:
        """Delete agent state."""
        await self.client.delete(
            agent_parameters_key(self.settings.REDIS_NAMESPACE, self.uuid)
        )

    @fault_tolerant(CommonSettings.redis_retry)
    async def subscribe(self) -> "MeasurementRoundRequest":
        """Subscribe to agent channels (all, specific) and wait for a response"""
        psub = self.client.pubsub()
        async with psub as p:
            await p.subscribe(
                agent_listen_key(self.settings.REDIS_NAMESPACE, self.uuid)
            )
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
