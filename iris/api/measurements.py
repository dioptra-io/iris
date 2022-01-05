"""Measurements operations."""
import asyncio
from typing import Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status
from sqlmodel import Session

from iris.api.authentication import assert_probing_enabled, current_verified_user
from iris.api.dependencies import get_redis, get_session, get_storage
from iris.api.validator import target_file_validator
from iris.commons.models.agent import Agent
from iris.commons.models.measurement import (
    Measurement,
    MeasurementCreate,
    MeasurementRead,
    MeasurementReadWithAgents,
)
from iris.commons.models.measurement_agent import (
    MeasurementAgent,
    MeasurementAgentCreate,
    MeasurementAgentState,
)
from iris.commons.models.pagination import Paginated
from iris.commons.models.target import Target
from iris.commons.models.user import UserDB
from iris.commons.redis import Redis
from iris.commons.storage import Storage, targets_key
from iris.worker.watch import watch_measurement_agent

router = APIRouter()


def assert_measurement_visibility(measurement: Measurement, user: UserDB):
    if not measurement or measurement.user_id != str(user.id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )


def set_or_raise(d, k, v):
    if k in d:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Multiple assignment of key `{k}`",
        )
    d[k] = v


def unfold_agent(
    active_agents: Dict[str, Agent], tagged_agent: MeasurementAgentCreate
) -> List[MeasurementAgentCreate]:
    """Transform a tagged agent in a list of agents with the corresponding UUIDs."""
    agents = []
    for uuid, active_agent in active_agents.items():
        if parameters := active_agent.parameters:
            if tagged_agent.tag in parameters.tags:
                tagged_agent = tagged_agent.copy()
                tagged_agent.tag = None
                tagged_agent.uuid = uuid
                agents.append(tagged_agent)
    return agents


@router.get(
    "/", response_model=Paginated[MeasurementRead], summary="Get all measurements."
)
async def get_measurements(
    request: Request,
    tag: Optional[str] = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    user: UserDB = Depends(current_verified_user),
    session: Session = Depends(get_session),
):
    assert_probing_enabled(user)
    count = Measurement.count(session, tag=tag, user_id=str(user.id))
    measurements = Measurement.all(
        session, tag=tag, user_id=str(user.id), offset=offset, limit=limit
    )
    measurements_ = MeasurementRead.from_measurements(measurements)
    return Paginated.from_results(request.url, measurements_, count, offset, limit)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=MeasurementReadWithAgents,
    summary="Request a measurement.",
)
async def post_measurement(
    measurement_body: MeasurementCreate = Body(
        ...,
        example={
            "tool": "diamond-miner",
            "agents": [
                {
                    "tag": "all",
                    "target_file": "prefixes.csv",
                }
            ],
            "tags": ["test"],
        },
    ),
    user: UserDB = Depends(current_verified_user),
    redis: Redis = Depends(get_redis),
    session: Session = Depends(get_session),
    storage: Storage = Depends(get_storage),
):
    assert_probing_enabled(user)
    active_agents = await redis.get_agents_by_uuid()

    agents = {}
    for agent in measurement_body.agents:
        if agent.uuid:
            if agent.uuid in active_agents:
                set_or_raise(agents, agent.uuid, agent)
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No agent associated with UUID {agent.uuid}",
                )
        else:
            if agents_ := unfold_agent(active_agents, agent):
                for agent_ in agents_:
                    set_or_raise(agents, agent_.uuid, agent_)
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No agents associated with tag {agent.tag}",
                )

    for agent in agents.values():
        global_min_ttl, global_max_ttl = await target_file_validator(
            storage,
            measurement_body.tool,
            user,
            agent.target_file,
            agent.tool_parameters.prefix_len_v4,
            agent.tool_parameters.prefix_len_v6,
        )
        agent.tool_parameters.global_min_ttl = global_min_ttl
        agent.tool_parameters.global_max_ttl = global_max_ttl

    measurement = Measurement(
        user_id=str(user.id),
        tool=measurement_body.tool,
        tags=measurement_body.tags,
    )
    session.add(measurement)
    session.commit()

    measurement_agents = [
        MeasurementAgent(
            measurement_uuid=measurement.uuid,
            agent_uuid=agent.uuid,
            agent_parameters=active_agents[agent.uuid].parameters,
            tool_parameters=agent.tool_parameters,
            probing_rate=agent.probing_rate,
            target_file=agent.target_file,
        )
        for agent in agents.values()
    ]
    session.add_all(measurement_agents)
    session.commit()

    for agent in agents.values():
        await storage.copy_file_to_bucket(
            storage.targets_bucket(str(user.id)),
            storage.archive_bucket(str(user.id)),
            agent.target_file,
            targets_key(measurement.uuid, agent.uuid),
        )

    for agent in agents.values():
        watch_measurement_agent.send(measurement.uuid, agent.uuid)

    return await get_measurement(
        measurement_uuid=UUID(measurement.uuid), user=user, session=session
    )


@router.get(
    "/{measurement_uuid}",
    response_model=MeasurementReadWithAgents,
    summary="Get measurement specified by UUID.",
)
async def get_measurement(
    measurement_uuid: UUID,
    user: UserDB = Depends(current_verified_user),
    session: Session = Depends(get_session),
):
    assert_probing_enabled(user)
    measurement = Measurement.get(session, str(measurement_uuid))
    assert_measurement_visibility(measurement, user)
    return MeasurementReadWithAgents.from_measurement(measurement)


@router.get(
    "/{measurement_uuid}/{agent_uuid}/target",
    response_model=Target,
    summary="Get target list of the measurement agent specified by UUID.",
)
async def get_measurement_agent_target(
    measurement_uuid: UUID,
    agent_uuid: UUID,
    user: UserDB = Depends(current_verified_user),
    storage: Storage = Depends(get_storage),
):
    assert_probing_enabled(user)
    target_file = await storage.get_file_no_retry(
        storage.archive_bucket(str(user.id)),
        targets_key(str(measurement_uuid), str(agent_uuid)),
    )
    # TODO: Return Target
    return [x.strip() for x in target_file["content"].split()]


@router.delete(
    "/{measurement_uuid}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel measurement specified by UUID.",
)
async def delete_measurement(
    measurement_uuid: UUID,
    user: UserDB = Depends(current_verified_user),
    redis: Redis = Depends(get_redis),
    session: Session = Depends(get_session),
):
    measurement = Measurement.get(session, str(measurement_uuid))
    aws = [
        delete_measurement_agent(
            measurement_uuid=UUID(agent.measurement_uuid),
            agent_uuid=UUID(agent.agent_uuid),
            user=user,
            redis=redis,
            session=session,
        )
        for agent in measurement.agents
    ]
    await asyncio.gather(*aws)


# TODO: Return Measurement/MeasurementAgent models in all routes.
@router.delete(
    "/{measurement_uuid}/{agent_uuid}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel measurement agent specified by UUID.",
)
async def delete_measurement_agent(
    measurement_uuid: UUID,
    agent_uuid: UUID,
    user: UserDB = Depends(current_verified_user),
    redis: Redis = Depends(get_redis),
    session: Session = Depends(get_session),
):
    assert_probing_enabled(user)
    measurement_agent = MeasurementAgent.get(
        session, str(measurement_uuid), str(agent_uuid)
    )
    assert_measurement_visibility(measurement_agent.measurement, user)
    await redis.cancel_measurement_agent(str(measurement_uuid), str(agent_uuid))
    measurement_agent.state = MeasurementAgentState.Canceled
    session.add(measurement_agent)
    session.commit()
