"""Measurements operations."""

from datetime import datetime
from typing import Dict
from uuid import UUID, uuid4

from diamond_miner.generator import count_prefixes
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status

from iris.api.pagination import DatabasePagination
from iris.api.schemas import (
    ExceptionResponse,
    MeasurementInfoResponse,
    MeasurementsDeleteResponse,
    MeasurementsGetResponse,
    MeasurementsPostBody,
    MeasurementsPostResponse,
    MeasurementsResultsResponse,
)
from iris.api.security import get_current_active_user
from iris.commons.database import Agents, MeasurementResults, Measurements
from iris.worker.hook import hook

router = APIRouter()


@router.get(
    "/", response_model=MeasurementsGetResponse, summary="Get all measurements."
)
async def get_measurements(
    request: Request,
    tag: str = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get all measurements."""
    database = Measurements(request.app.settings, request.app.logger)

    querier = DatabasePagination(database, request, offset, limit)
    output = await querier.query(user=user["username"], tag=tag)

    measurements = []
    for measurement in output["results"]:
        state = await request.app.redis.get_measurement_state(measurement["uuid"])
        measurements.append(
            {
                "uuid": measurement["uuid"],
                "state": measurement["state"] if state is None else state,
                "tool": measurement["tool"],
                "tags": measurement["tags"],
                "start_time": measurement["start_time"],
                "end_time": measurement["end_time"],
            }
        )

    output["results"] = measurements

    return output


async def verify_quota(tool, content, user_quota):
    """Verify that the quota is not exceeded."""
    targets = [p.strip() for p in content.split()]
    if tool in ["diamond-miner", "yarrp"]:
        n_prefixes = count_prefixes([target.split(",")[0] for target in targets])
    elif tool == "ping":
        n_prefixes = count_prefixes(
            [target.split(",")[0] for target in targets],
            prefix_len_v4=32,
            prefix_len_v6=128,
        )
    else:
        raise ValueError("Unrecognized tool")
    return n_prefixes <= user_quota


async def target_file_validator(request, tool, user, target_file):
    """Validate the target file input."""

    # Verify that the target file exists on AWS S3
    try:
        target_file = await request.app.storage.get_file_no_retry(
            request.app.settings.AWS_S3_TARGETS_BUCKET_PREFIX + user["username"],
            target_file,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Target file not found"
        )

    # Do not check if the target file is a custom probe file
    if target_file["key"].endswith(".probes"):
        if tool != "yarrp":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only `yarrp` tool can be used with custom probe file",
            )
        return None, None

    # Check if the user respects his quota
    try:
        is_quota_respected = await verify_quota(
            tool, target_file["content"], user["quota"]
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
        if tool == "ping" and protocol == "udp":
            # Disabling UDP port scanning abilities
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Tool `ping` only accessible with ICMP protocol",
            )
    return global_min_ttl, global_max_ttl


def tool_parameters_validator(tool, tool_parameters):
    """Validate tool parameters."""
    # Specific checks for `diamond-miner`
    if tool == "diamond-miner":
        tool_parameters["n_flow_ids"] = 6

    # Specific checks for `yarrp`
    if tool == "yarrp":
        tool_parameters["n_flow_ids"] = 1
        tool_parameters["max_round"] = 1

    # Specific checks for `ping`
    if tool == "ping":
        tool_parameters["max_round"] = 1
        tool_parameters["n_flow_ids"] = 1

    return tool_parameters


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=MeasurementsPostResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Request a measurement.",
)
async def post_measurement(
    request: Request,
    measurement: MeasurementsPostBody = Body(
        ...,
        example={
            "tool": "diamond-miner",
            "agents": [
                {
                    "uuid": "ddd8541d-b4f5-42ce-b163-e3e9bfcd0a47",
                    "target_file": "prefixes.csv",
                }
            ],
            "tags": ["test"],
        },
    ),
    user: Dict = Depends(get_current_active_user),
):
    """Request a measurement."""
    # Get all connected agents
    active_agents = await request.app.redis.get_agents(state=False, parameters=True)
    active_agent_uuids = [agent["uuid"] for agent in active_agents]

    agents = {}
    for agent in measurement.agents:
        # Check if the agent exists
        agent_uuid = str(agent.uuid)
        if agent_uuid not in active_agent_uuids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found"
            )

        # Check agent target file
        global_min_ttl, global_max_ttl = await target_file_validator(
            request, measurement.tool, user, agent.target_file
        )

        # Check tool parameters
        agent.tool_parameters = tool_parameters_validator(
            measurement.tool, agent.tool_parameters.dict()
        )
        agent.tool_parameters["global_min_ttl"] = global_min_ttl
        agent.tool_parameters["global_max_ttl"] = global_max_ttl
        agents[agent_uuid] = agent.dict()
        del agents[agent_uuid]["uuid"]

    measurement = measurement.dict()
    del measurement["agents"]

    # Add mesurement metadata
    measurement["measurement_uuid"] = str(uuid4())
    measurement["user"] = user["username"]
    measurement["start_time"] = datetime.timestamp(datetime.now())

    # launch a measurement procedure on the worker.
    hook.send(agents, measurement)

    return {"uuid": measurement["measurement_uuid"]}


@router.get(
    "/{measurement_uuid}",
    response_model=MeasurementInfoResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get measurement information by uuid.",
)
async def get_measurement_by_uuid(
    request: Request,
    measurement_uuid: UUID,
    user: Dict = Depends(get_current_active_user),
):
    """Get measurement information by uuid."""
    measurement = await Measurements(request.app.settings, request.app.logger).get(
        user["username"], measurement_uuid
    )
    if measurement is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    state = await request.app.redis.get_measurement_state(measurement_uuid)
    measurement["state"] = state if state is not None else measurement["state"]

    agents_info = await Agents(request.app.settings, request.app.logger).all(
        measurement["uuid"]
    )

    agents = []
    for agent_info in agents_info:
        if measurement["state"] == "waiting":
            agent_info["state"] = "waiting"

        try:
            target_file = await request.app.storage.get_file_no_retry(
                request.app.settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + user["username"],
                f"targets__{measurement['uuid']}__{agent_info['uuid']}.csv",
            )
            target_file_content = [c.strip() for c in target_file["content"].split()]
            if len(target_file_content) > 100:
                # NOTE: Don't display the measurement if the file is too big
                # to avoid to slow down the API
                target_file_content = []
        except Exception:
            target_file_content = []

        agents.append(
            {
                "uuid": agent_info["uuid"],
                "state": agent_info["state"],
                "specific": {
                    "target_file": agent_info["target_file"],
                    "target_file_content": target_file_content,
                    "probing_rate": agent_info["probing_rate"],
                    "tool_parameters": agent_info["tool_parameters"],
                },
                "parameters": agent_info["agent_parameters"],
            }
        )
    measurement["agents"] = agents

    return measurement


@router.delete(
    "/{measurement_uuid}",
    response_model=MeasurementsDeleteResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Cancel measurement",
)
async def delete_measurement(
    request: Request,
    measurement_uuid: UUID,
    user: Dict = Depends(get_current_active_user),
):
    """Cancel a measurement."""
    measurement_info = await Measurements(request.app.settings, request.app.logger).get(
        user["username"], measurement_uuid
    )
    if measurement_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    state = await request.app.redis.get_measurement_state(measurement_uuid)
    if state is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement already finished"
        )

    await request.app.redis.set_measurement_state(measurement_uuid, "canceled")
    return {"uuid": measurement_uuid, "action": "canceled"}


@router.get(
    "/{measurement_uuid}/{agent_uuid}",
    response_model=MeasurementsResultsResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get measurement results.",
)
async def get_measurement_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    user: Dict = Depends(get_current_active_user),
):
    """Get measurement results."""
    measurement_info = await Measurements(request.app.settings, request.app.logger).get(
        user["username"], measurement_uuid
    )
    if measurement_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    agent_info = await Agents(request.app.settings, request.app.logger).get(
        measurement_uuid, agent_uuid
    )

    if agent_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"The agent `{agent_uuid}` "
                f"did not participate to measurement `{measurement_uuid}`"
            ),
        )

    if agent_info["state"] != "finished":
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=(
                f"The agent `{agent_uuid}` "
                f"has not finished the measurement `{measurement_uuid}`"
            ),
        )

    database = MeasurementResults(
        request.app.settings, request.app.logger, measurement_uuid, agent_uuid
    )

    is_table_exists = await database.exists()
    if not is_table_exists:
        return {"count": 0, "next": None, "previous": None, "results": []}

    querier = DatabasePagination(database, request, offset, limit)
    return await querier.query()
