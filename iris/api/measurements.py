"""Measurements operations."""

from typing import List, Set
from uuid import UUID

from diamond_miner.generators import count_prefixes
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status

from iris.api.dependencies import get_database, get_redis, get_storage, settings
from iris.api.pagination import DatabasePagination
from iris.api.security import get_current_active_user
from iris.commons.database import Agents, Database, Measurements
from iris.commons.redis import Redis
from iris.commons.schemas import private, public
from iris.commons.storage import Storage
from iris.worker.hook import hook

router = APIRouter()


@router.get(
    "/",
    response_model=public.Paginated[public.MeasurementSummary],
    summary="Get all measurements.",
)
async def get_measurements(
    request: Request,
    tag: str = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    user: public.Profile = Depends(get_current_active_user),
    database: Database = Depends(get_database),
    redis: Redis = Depends(get_redis),
):
    """Get all measurements."""
    querier = DatabasePagination(Measurements(database), request, offset, limit)
    output = await querier.query(user=user.username, tag=tag)

    measurements: List[public.Measurement] = output["results"]
    summaries: List[public.MeasurementSummary] = []

    for measurement in measurements:
        state = await redis.get_measurement_state(measurement.uuid)
        if not state or state == public.MeasurementState.Unknown:
            state = measurement.state
        summaries.append(
            public.MeasurementSummary(
                uuid=measurement.uuid,
                state=state,
                tool=measurement.tool,
                tags=measurement.tags,
                start_time=measurement.start_time,
                end_time=measurement.end_time,
            )
        )

    output["results"] = summaries

    return output


async def verify_quota(tool, content, user_quota):
    """Verify that the quota is not exceeded."""
    targets = [p.strip() for p in content.split()]
    if tool in [public.Tool.DiamondMiner, public.Tool.Yarrp]:
        n_prefixes = count_prefixes([target.split(",")[0] for target in targets])
    elif tool == public.Tool.Ping:
        n_prefixes = count_prefixes(
            [target.split(",")[0] for target in targets],
            prefix_len_v4=32,
            prefix_len_v6=128,
        )
    else:
        raise ValueError("Unrecognized tool")
    return n_prefixes <= user_quota


async def target_file_validator(
    request,
    storage: Storage,
    tool: public.Tool,
    user: public.Profile,
    target_filename: str,
):
    """Validate the target file input."""

    # Verify that the target file exists on AWS S3
    try:
        target_file = await storage.get_file_no_retry(
            settings.AWS_S3_TARGETS_BUCKET_PREFIX + user.username,
            target_filename,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Target file not found"
        )

    # Do not check if the target file is a custom probe file
    if target_file["key"].endswith(".probes"):
        if tool != public.Tool.Yarrp:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only `yarrp` tool can be used with custom probe file",
            )
        return None, None

    # Check if the user respects his quota
    try:
        is_quota_respected = await verify_quota(
            tool, target_file["content"], user.quota
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid prefixes length"
        )
    if not is_quota_respected:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Quota exceeded",
        )

    # Check protocol and min/max TTL
    global_min_ttl = 256
    global_max_ttl = 0
    for line in [p.strip() for p in target_file["content"].split()]:
        _, protocol, min_ttl, max_ttl = line.split(",")
        min_ttl, max_ttl = int(min_ttl), int(max_ttl)
        global_min_ttl = min(global_min_ttl, min_ttl)
        global_max_ttl = max(global_max_ttl, max_ttl)
        if tool == public.Tool.Ping and protocol == "udp":
            # Disabling UDP port scanning abilities
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Tool `ping` only accessible with ICMP protocol",
            )
    return global_min_ttl, global_max_ttl


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=public.MeasurementPostResponse,
    responses={404: {"model": public.GenericException}},
    summary="Request a measurement.",
)
async def post_measurement(
    request: Request,
    measurement: public.MeasurementPostBody = Body(
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
    user: public.Profile = Depends(get_current_active_user),
    redis: Redis = Depends(get_redis),
    storage: Storage = Depends(get_storage),
):
    """Request a measurement."""
    active_agents = await redis.get_agents_by_uuid()
    print(user)

    # Update the list of requested agents to include agents selected by tag.
    agents: List[public.MeasurementAgentPostBody] = []

    for agent in measurement.agents:
        if agent.uuid:
            agents.append(agent)
        else:
            at_least_one = False
            for uuid, active_agent in active_agents.items():
                if (
                    active_agent.parameters
                    and agent.tag in active_agent.parameters.agent_tags
                ):
                    # Matching agent for tag found, replace tag field with uuid field
                    agents.append(agent.copy(exclude={"tag"}, update={"uuid": uuid}))
                    at_least_one = True
            if not at_least_one:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No agent associated with tag {agent.tag}",
                )

    # Keep track of the registered agents to make sure that an agent is not
    # registered twice. e.g. with two overlapping agent tags.
    registered_agents: Set[UUID] = set()

    for agent in agents:
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
        registered_agents.add(agent.uuid)

        # Check agent target file
        global_min_ttl, global_max_ttl = await target_file_validator(
            request, storage, measurement.tool, user, agent.target_file
        )
        agent.tool_parameters.global_min_ttl = global_min_ttl
        agent.tool_parameters.global_max_ttl = global_max_ttl

        # Enforce some tool specific parameters
        # TODO: Can we do this with pydantic?
        if measurement.tool == public.Tool.DiamondMiner:
            agent.tool_parameters.n_flow_ids = 6
        if measurement.tool in (public.Tool.Ping, public.Tool.Yarrp):
            agent.tool_parameters.n_flow_ids = 1
            agent.tool_parameters.max_round = 1

    # Update the agents list and set private metadata.
    measurement_request = private.MeasurementRequest(
        **measurement.dict(exclude={"agents"}), agents=agents, username=user.username
    )

    # Launch a measurement procedure on the worker.
    hook.send(measurement_request)

    return public.MeasurementPostResponse(uuid=measurement_request.uuid)


@router.get(
    "/{measurement_uuid}",
    response_model=public.Measurement,
    responses={404: {"model": public.GenericException}},
    summary="Get measurement specified by UUID.",
)
async def get_measurement_by_uuid(
    request: Request,
    measurement_uuid: UUID,
    user: public.Profile = Depends(get_current_active_user),
    database: Database = Depends(get_database),
    redis: Redis = Depends(get_redis),
    storage: Storage = Depends(get_storage),
):
    """Get measurement information by uuid."""
    measurement = await Measurements(database).get(user.username, measurement_uuid)
    if measurement is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    state = await redis.get_measurement_state(measurement_uuid)
    if state and state != public.MeasurementState.Unknown:
        measurement.state = state

    agents_info = await Agents(database).all(measurement.uuid)
    for agent_info in agents_info:
        if measurement.state == public.MeasurementState.Waiting:
            agent_info.state = public.MeasurementState.Waiting
        try:
            target_file = await storage.get_file_no_retry(
                settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + user.username,
                f"targets__{measurement.uuid}__{agent_info.uuid}.csv",
            )
            target_file_content = [c.strip() for c in target_file["content"].split()]
            # NOTE: Don't display the measurement if the file is too big
            # to avoid to slow down the API.
            if len(target_file_content) <= 100:
                agent_info.specific.target_file_content = target_file_content
        except Exception:
            pass
        measurement.agents.append(agent_info)

    return measurement


@router.delete(
    "/{measurement_uuid}",
    response_model=public.MeasurementDeleteResponse,
    responses={404: {"model": public.GenericException}},
    summary="Cancel measurement specified by UUID.",
)
async def delete_measurement(
    request: Request,
    measurement_uuid: UUID,
    user: public.Profile = Depends(get_current_active_user),
    database: Database = Depends(get_database),
    redis: Redis = Depends(get_redis),
):
    """Cancel a measurement."""
    measurement_info = await Measurements(database).get(user.username, measurement_uuid)
    if measurement_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    state = await redis.get_measurement_state(measurement_uuid)
    if state is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement already finished"
        )

    await redis.set_measurement_state(
        measurement_uuid, public.MeasurementState.Canceled
    )
    return public.MeasurementDeleteResponse(uuid=measurement_uuid, action="canceled")
