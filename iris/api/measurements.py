"""Measurements operations."""

from typing import Dict, List, Optional
from uuid import UUID

from diamond_miner.generators.standalone import count_prefixes
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status
from sqlalchemy.engine import Engine

from iris.api.authentication import current_verified_user
from iris.api.dependencies import get_database, get_engine, get_redis, get_storage
from iris.commons.database import Database, agents
from iris.commons.redis import Redis
from iris.commons.schemas.exceptions import GenericException
from iris.commons.schemas.measurements import (
    Measurement,
    MeasurementAgentPostBody,
    MeasurementDeleteResponse,
    MeasurementPostBody,
    MeasurementPostResponse,
    MeasurementRequest,
    MeasurementState,
    MeasurementSummary,
    Tool,
)
from iris.commons.schemas.paging import Paginated
from iris.commons.schemas.users import UserDB
from iris.commons.storage import Storage
from iris.worker.hook import hook

router = APIRouter()


def assert_probing_enabled(user: UserDB):
    if not user.probing_enabled:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You must have probing enabled to access this resource",
        )


def assert_measurement_visibility(measurement: Measurement, user: UserDB):
    if not measurement or measurement.user_id != user.id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )


@router.get(
    "/",
    response_model=Paginated[MeasurementSummary],
    summary="Get all measurements.",
)
async def get_measurements(
    request: Request,
    tag: Optional[str] = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    engine: Engine = Depends(get_engine),
    user: UserDB = Depends(current_verified_user),
    redis: Redis = Depends(get_redis),
):
    """Get all measurements."""
    assert_probing_enabled(user)

    count = Measurement.count(engine, tag=tag, user_id=user.id)
    measurements = Measurement.all(
        engine, tag=tag, user_id=user.id, offset=offset, limit=limit
    )
    summaries: List[MeasurementSummary] = []

    for measurement in measurements:
        state = await redis.get_measurement_state(measurement.uuid)
        if not state or state == MeasurementState.Unknown:
            state = measurement.state
        summaries.append(
            MeasurementSummary(
                uuid=measurement.uuid,
                state=state,
                tool=measurement.tool,
                tags=measurement.tags,
                start_time=measurement.start_time,
                end_time=measurement.end_time,
            )
        )

    return Paginated.from_results(request.url, summaries, count, offset, limit)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=MeasurementPostResponse,
    responses={404: {"model": GenericException}},
    summary="Request a measurement.",
)
async def post_measurement(
    measurement: MeasurementPostBody = Body(
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
    storage: Storage = Depends(get_storage),
):
    """Request a measurement."""
    assert_probing_enabled(user)
    active_agents = await redis.get_agents_by_uuid()

    # Update the list of requested agents to include agents selected by tag.
    agents_: List[MeasurementAgentPostBody] = []

    for agent in measurement.agents:
        if agent.uuid:
            agents_.append(agent)
        else:
            at_least_one = False
            for uuid, active_agent in active_agents.items():
                if (
                    active_agent.parameters
                    and agent.tag in active_agent.parameters.agent_tags
                ):
                    # Matching agent for tag found, replace tag field with uuid field
                    agents_.append(agent.copy(exclude={"tag"}, update={"uuid": uuid}))
                    at_least_one = True
            if not at_least_one:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No agent associated with tag {agent.tag}",
                )

    # Keep track of the registered agents to make sure that an agent is not
    # registered twice. e.g. with two overlapping agent tags.
    registered_agents: Dict[UUID, MeasurementAgentPostBody] = {}

    for agent in agents_:
        # Ensure that the agent exists
        if agent.uuid not in active_agents:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No agent associated with UUID {agent.uuid}",
            )

        # Ensure that the agent has not already been registered
        if agent.uuid in registered_agents:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Multiple definition of agent `{agent.uuid}`",
            )

        # TODO: Check probing limit
        # Check agent target file is compatible with the measurement's parameters
        global_min_ttl, global_max_ttl = await target_file_validator(
            storage,
            measurement.tool,
            user,
            agent.target_file,
            agent.tool_parameters.prefix_len_v4,
            agent.tool_parameters.prefix_len_v6,
        )

        registered_agents[agent.uuid] = agent.copy(
            update={
                "tool_parameters": agent.tool_parameters.copy(
                    update={
                        "global_min_ttl": global_min_ttl,
                        "global_max_ttl": global_max_ttl,
                    }
                )
            }
        )

    # Update the agents list and set private metadata.
    measurement_request = MeasurementRequest(
        **measurement.dict(exclude={"agents"}),
        agents=list(registered_agents.values()),
        user_id=user.id,
    )

    # Launch a measurement procedure on the worker.
    hook.send(measurement_request)

    return MeasurementPostResponse(uuid=measurement_request.uuid)


@router.get(
    "/{measurement_uuid}",
    response_model=Measurement,
    responses={404: {"model": GenericException}},
    summary="Get measurement specified by UUID.",
)
async def get_measurement_by_uuid(
    measurement_uuid: UUID,
    user: UserDB = Depends(current_verified_user),
    database: Database = Depends(get_database),
    engine: Engine = Depends(get_engine),
    redis: Redis = Depends(get_redis),
    storage: Storage = Depends(get_storage),
):
    """Get measurement information by uuid."""
    assert_probing_enabled(user)
    measurement = Measurement.get(engine, measurement_uuid)
    assert_measurement_visibility(measurement, user)

    state = await redis.get_measurement_state(measurement_uuid)
    if state and state != MeasurementState.Unknown:
        measurement = measurement.copy(update={"state": state})

    measurement_agents = []
    agents_info = await agents.all(database, measurement.uuid)

    for agent_info in agents_info:
        if measurement.state == MeasurementState.Waiting:
            agent_info = agent_info.copy(update={"state": measurement.state})
        try:
            target_file = await storage.get_file_no_retry(
                storage.archive_bucket(user.id),
                f"targets__{measurement.uuid}__{agent_info.uuid}.csv",
            )
            target_file_content = [c.strip() for c in target_file["content"].split()]
            # NOTE: Don't display the measurement if the file is too big
            # to avoid to slow down the API.
            if len(target_file_content) <= 100:
                agent_info = agent_info.copy(
                    update={
                        "specific": agent_info.specific.copy(
                            update={"target_file_content": target_file_content}
                        )
                    }
                )
        except Exception:
            pass
        measurement_agents.append(agent_info)

    return measurement.copy(update={"agents": measurement_agents})


@router.delete(
    "/{measurement_uuid}",
    response_model=MeasurementDeleteResponse,
    responses={404: {"model": GenericException}},
    summary="Cancel measurement specified by UUID.",
)
async def delete_measurement(
    measurement_uuid: UUID,
    user: UserDB = Depends(current_verified_user),
    engine: Engine = Depends(get_engine),
    redis: Redis = Depends(get_redis),
):
    """Cancel a measurement."""
    assert_probing_enabled(user)
    measurement = Measurement.get(engine, measurement_uuid)
    assert_measurement_visibility(measurement, user)

    state = await redis.get_measurement_state(measurement_uuid)
    if not state:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Measurement already finished",
        )

    await redis.set_measurement_state(measurement_uuid, MeasurementState.Canceled)
    return MeasurementDeleteResponse(uuid=measurement_uuid, action="canceled")


async def target_file_validator(
    storage: Storage,
    tool: Tool,
    user: UserDB,
    target_filename: str,
    prefix_len_v4: int,
    prefix_len_v6: int,
):
    """Validate the target file input."""
    # Check validation for "Probe" tool
    # The user must be admin and the target file must have the proper metadata
    if tool == Tool.Probes:
        # Verify that the target file exists on S3
        try:
            target_file = await storage.get_file_no_retry(
                storage.targets_bucket(user.id),
                target_filename,
                retrieve_content=False,
            )
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Probe file not found"
            )

        # Check if the user is admin
        if not user.is_superuser:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin priviledges required",
            )

        # Check if the metadata is correct
        if not target_file["metadata"] or not (
            target_file["metadata"].get("is_probes_file")
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Target file specified is not a probe file",
            )
        return 0, 255

    # Verify that the target file exists on S3
    try:
        target_file = await storage.get_file_no_retry(
            storage.targets_bucket(user.id),
            target_filename,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Target file not found"
        )

    # Check if the prefixes respect the tool prefix length
    try:
        count_prefixes(
            (p.split(",")[0].strip() for p in target_file["content"].split()),
            prefix_len_v4=prefix_len_v4,
            prefix_len_v6=prefix_len_v6,
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid prefixes length"
        )

    # Check protocol and min/max TTL
    global_min_ttl = 256
    global_max_ttl = 0
    for line in [p.strip() for p in target_file["content"].split()]:
        _, protocol, min_ttl, max_ttl, n_initial_flows = line.split(",")
        min_ttl, max_ttl = int(min_ttl), int(max_ttl)
        global_min_ttl = min(global_min_ttl, min_ttl)
        global_max_ttl = max(global_max_ttl, max_ttl)
        if tool == Tool.Ping and protocol == "udp":
            # Disabling UDP port scanning abilities
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Tool `ping` only accessible with ICMP protocol",
            )
    return global_min_ttl, global_max_ttl
