"""Measurements operations."""
import asyncio
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status
from sqlmodel import Session

from iris.api.authentication import current_active_user
from iris.api.settings import APISettings
from iris.api.validator import target_file_validator
from iris.commons.dependencies import get_redis, get_session, get_settings, get_storage
from iris.commons.models import (
    Agent,
    Measurement,
    MeasurementAgent,
    MeasurementAgentCreate,
    MeasurementAgentRead,
    MeasurementAgentState,
    MeasurementCreate,
    MeasurementPatch,
    MeasurementRead,
    MeasurementReadWithAgents,
    Paginated,
    Target,
    User,
)
from iris.commons.redis import Redis
from iris.commons.storage import Storage, targets_key
from iris.commons.utils import unwrap
from iris.worker.watch import watch_measurement_agent

router = APIRouter()


def assert_measurement_visibility(
    measurement: Measurement | None,
    user: User,
    settings: APISettings,
) -> None:
    if not measurement or (
        measurement.user_id != str(user.id) and not user.is_superuser
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )


def assert_measurement_agent_visibility(
    measurement_agent: MeasurementAgent | None, user: User
) -> None:
    if not measurement_agent or (
        measurement_agent.measurement.user_id != str(user.id) and not user.is_superuser
    ):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement agent not found"
        )


def set_or_raise(d, k, v):
    if k in d:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Multiple assignment of key `{k}`",
        )
    d[k] = v


def unfold_agent(
    active_agents: dict[str, Agent], tagged_agent: MeasurementAgentCreate
) -> list[MeasurementAgentCreate]:
    """Transform a tagged agent in a list of agents with the corresponding UUIDs."""
    agents = []
    for uuid, active_agent in active_agents.items():
        if parameters := active_agent.parameters:
            if tagged_agent.tag in parameters.tags:
                agents.append(tagged_agent.copy(update={"tag": None, "uuid": uuid}))
    return agents


@router.get(
    "/", response_model=Paginated[MeasurementRead], summary="Get all measurements."
)
async def get_measurements(
    request: Request,
    state: MeasurementAgentState | None = None,
    tag: str | None = None,
    only_mine: bool = True,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    user: User = Depends(current_active_user),
    session: Session = Depends(get_session),
):
    if not only_mine and not user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not allowed to fetch measurements from other users.",
        )
    tags = []
    if tag:
        tags.append(tag)
    user_id = str(user.id) if only_mine else None
    count = Measurement.count(session, state=state, tags=tags, user_id=user_id)
    measurements = Measurement.all(
        session,
        state=state,
        tags=tags,
        user_id=user_id,
        offset=offset,
        limit=limit,
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
        examples=[
            {
                "tool": "diamond-miner",
                "agents": [
                    {
                        "tag": "all",
                        "target_file": "prefixes.csv",
                    }
                ],
                "tags": ["test"],
            }
        ],
    ),
    user: User = Depends(current_active_user),
    redis: Redis = Depends(get_redis),
    session: Session = Depends(get_session),
    storage: Storage = Depends(get_storage),
    settings: APISettings = Depends(get_settings),
):
    active_agents = await redis.get_agents_by_uuid()

    agents: dict[str, MeasurementAgentCreate] = {}
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
            agent.tool_parameters,
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
            agent_parameters=active_agents[unwrap(agent.uuid)].parameters,
            tool_parameters=agent.tool_parameters,
            batch_size=agent.batch_size,
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
            targets_key(measurement.uuid, unwrap(agent.uuid)),
        )

    for agent in agents.values():
        assert agent.uuid  # make mypy happy
        watch_measurement_agent.send(measurement.uuid, unwrap(agent.uuid))

    return await get_measurement(
        measurement_uuid=UUID(measurement.uuid),
        user=user,
        session=session,
        settings=settings,
    )


@router.get(
    "/{measurement_uuid}",
    response_model=MeasurementReadWithAgents,
    summary="Get measurement specified by UUID.",
)
async def get_measurement(
    measurement_uuid: UUID,
    user: User = Depends(current_active_user),
    session: Session = Depends(get_session),
    settings: APISettings = Depends(get_settings),
):
    measurement = Measurement.get(session, str(measurement_uuid))
    assert_measurement_visibility(measurement, user, settings)
    return MeasurementReadWithAgents.from_measurement(measurement)


@router.patch(
    "/{measurement_uuid}",
    response_model=MeasurementReadWithAgents,
    summary="Patch measurement specified by UUID.",
)
async def patch_measurement(
    measurement_uuid: UUID,
    measurement_body: MeasurementPatch = Body(
        ...,
        examples=[
            {
                "tags": ["test"],
            }
        ],
    ),
    user: User = Depends(current_active_user),
    session: Session = Depends(get_session),
    settings: APISettings = Depends(get_settings),
):
    measurement = Measurement.get(session, str(measurement_uuid))
    assert_measurement_visibility(measurement, user, settings)
    if tags := measurement_body.tags:
        measurement.set_tags(session, tags)
    return await get_measurement(
        measurement_uuid=measurement_uuid,
        user=user,
        session=session,
        settings=settings,
    )


@router.get(
    "/{measurement_uuid}/{agent_uuid}/target",
    response_model=Target,
    summary="Get target list of the measurement agent specified by UUID.",
)
async def get_measurement_agent_target(
    measurement_uuid: UUID,
    agent_uuid: UUID,
    user: User = Depends(current_active_user),
    session: Session = Depends(get_session),
    settings: APISettings = Depends(get_settings),
    storage: Storage = Depends(get_storage),
):
    measurement = Measurement.get(session, str(measurement_uuid))
    assert_measurement_visibility(measurement, user, settings)
    target_file = await storage.get_file_no_retry(
        storage.archive_bucket(measurement.user_id),
        targets_key(str(measurement_uuid), str(agent_uuid)),
    )
    return Target.from_s3(target_file)


@router.delete(
    "/{measurement_uuid}",
    response_model=MeasurementReadWithAgents,
    summary="Cancel measurement specified by UUID.",
)
async def cancel_measurement(
    measurement_uuid: UUID,
    user: User = Depends(current_active_user),
    redis: Redis = Depends(get_redis),
    session: Session = Depends(get_session),
    settings: APISettings = Depends(get_settings),
):
    measurement = Measurement.get(session, str(measurement_uuid))
    assert_measurement_visibility(measurement, user, settings)
    aws = [
        cancel_measurement_agent(
            measurement_uuid=UUID(agent.measurement_uuid),
            agent_uuid=UUID(agent.agent_uuid),
            user=user,
            redis=redis,
            session=session,
        )
        for agent in measurement.agents
    ]
    await asyncio.gather(*aws)
    return await get_measurement(
        measurement_uuid=measurement_uuid,
        user=user,
        session=session,
        settings=settings,
    )


@router.delete(
    "/{measurement_uuid}/{agent_uuid}",
    response_model=MeasurementAgentRead,
    summary="Cancel measurement agent specified by UUID.",
)
async def cancel_measurement_agent(
    measurement_uuid: UUID,
    agent_uuid: UUID,
    user: User = Depends(current_active_user),
    redis: Redis = Depends(get_redis),
    session: Session = Depends(get_session),
):
    measurement_agent = MeasurementAgent.get(
        session, str(measurement_uuid), str(agent_uuid)
    )
    assert_measurement_agent_visibility(measurement_agent, user)
    await redis.delete_request(str(measurement_uuid), str(agent_uuid))
    measurement_agent.state = MeasurementAgentState.Canceled
    measurement_agent.end_time = datetime.utcnow()
    session.add(measurement_agent)
    session.commit()
    return measurement_agent
