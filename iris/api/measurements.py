"""Measurements operations."""

from datetime import datetime
from uuid import UUID, uuid4

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
from iris.api.security import authenticate
from iris.commons.database import (
    DatabaseAgents,
    DatabaseAgentsSpecific,
    DatabaseMeasurementResults,
    DatabaseMeasurements,
    DatabaseUsers,
    get_session,
)
from iris.worker.hook import hook

router = APIRouter()


@router.get("/", response_model=MeasurementsGetResponse, summary="Get all measurements")
async def get_measurements(
    request: Request,
    tag: str = None,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    username: str = Depends(authenticate),
):
    """Get all measurements."""
    session = get_session(request.app.settings)
    database = DatabaseMeasurements(session, request.app.settings, request.app.logger)

    querier = DatabasePagination(database, request, offset, limit)
    output = await querier.query(user=username, tag=tag)

    # Refine measurements output
    measurements = []
    for measurement in output["results"]:
        state = await request.app.redis.get_measurement_state(measurement["uuid"])
        measurements.append(
            {
                "uuid": measurement["uuid"],
                "state": "finished" if state is None else state,
                "tool": measurement["tool"],
                "tags": measurement["tags"],
                "start_time": measurement["start_time"],
                "end_time": measurement["end_time"],
            }
        )

    output["results"] = measurements

    return output


def tool_parameters_validator(tool, parameters):
    """Validate tool parameters."""
    # Specific checks for `diamond-miner-ping`
    if tool == "diamond-miner-ping":
        parameters.max_round = 1
        # Disabling UDP port scanning abilities
        if parameters.protocol == "udp":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Tool `diamond-miner-ping` only accessible with ICMP protocol",
            )
    return parameters


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=MeasurementsPostResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Request a measurement",
)
async def post_measurement(
    request: Request,
    measurement: MeasurementsPostBody = Body(
        ...,
        example={
            "targets_file": "prefixes.txt",
            "tool": "diamond-miner",
            "tool_parameters": {"protocol": "udp", "min_ttl": 2, "max_ttl": 30},
            "tags": ["test"],
        },
    ),
    username: str = Depends(authenticate),
):
    """Request a measurement."""
    session = get_session(request.app.settings)
    users_database = DatabaseUsers(session, request.app.settings, request.app.logger)

    # Check if a user is active
    user_info = await users_database.get(username)
    if not user_info["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Account inactive",
        )

    # Verify that the targets file exists on AWS S3
    try:
        await request.app.storage.get_file_no_retry(
            request.app.settings.AWS_S3_TARGETS_BUCKET_PREFIX + username,
            measurement.targets_file,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File object not found"
        )

    measurement.tool_parameters = tool_parameters_validator(
        measurement.tool, measurement.tool_parameters
    )

    # Get all connected agents
    active_agents = await request.app.redis.get_agents(state=False, parameters=False)
    active_agents = [agent["uuid"] for agent in active_agents]

    # Filter out by `agents` key input if provided
    agents = {}
    if measurement.agents:
        for agent in measurement.agents:
            agent_uuid = str(agent.uuid)
            if agent_uuid not in active_agents:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found"
                )
            agent.tool_parameters = tool_parameters_validator(
                measurement.tool, agent.tool_parameters
            )
            agent.tool_parameters = agent.tool_parameters.dict(exclude_unset=True)
            agents[agent_uuid] = agent.dict()
            del agents[agent_uuid]["uuid"]
    else:
        agents = {uuid: {} for uuid in active_agents}

    measurement = measurement.dict()
    measurement["tool_parameters"] = dict(measurement["tool_parameters"])
    del measurement["agents"]

    # Add mesurement metadata
    measurement["measurement_uuid"] = str(uuid4())
    measurement["user"] = username
    measurement["start_time"] = datetime.timestamp(datetime.now())

    # launch a measurement procedure on the worker.
    hook.send(agents, measurement)

    return {"uuid": measurement["measurement_uuid"]}


@router.get(
    "/{measurement_uuid}",
    response_model=MeasurementInfoResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get measurement information by uuid",
)
async def get_measurement_by_uuid(
    request: Request,
    measurement_uuid: UUID,
    username: str = Depends(authenticate),
):
    """Get measurement information by uuid."""
    session = get_session(request.app.settings)
    measurement = await DatabaseMeasurements(
        session, request.app.settings, request.app.logger
    ).get(username, measurement_uuid)
    if measurement is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    state = await request.app.redis.get_measurement_state(measurement_uuid)
    measurement["state"] = "finished" if state is None else state

    agents_specific = await DatabaseAgentsSpecific(
        session, request.app.settings, request.app.logger
    ).all(measurement["uuid"])

    agents = []
    for agent_specific in agents_specific:
        agent_info = await DatabaseAgents(
            session, request.app.settings, request.app.logger
        ).get(agent_specific["uuid"])

        if measurement["state"] == "waiting":
            agent_specific["state"] = "waiting"
        elif measurement["state"] == "finished":
            agent_specific["state"] = "finished"

        agents.append(
            {
                "uuid": agent_specific["uuid"],
                "state": agent_specific["state"],
                "specific": {
                    "targets_file": agent_specific["targets_file"],
                    "probing_rate": agent_specific["probing_rate"],
                    "tool_parameters": agent_specific["tool_parameters"],
                },
                "parameters": {
                    "version": agent_info["version"],
                    "hostname": agent_info["hostname"],
                    "ip_address": agent_info["ip_address"],
                },
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
    username: str = Depends(authenticate),
):
    """Cancel a measurement."""
    session = get_session(request.app.settings)

    measurement_info = await DatabaseMeasurements(
        session, request.app.settings, request.app.logger
    ).get(username, measurement_uuid)
    if measurement_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    state = await request.app.redis.get_measurement_state(measurement_uuid)
    if state is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement already finished"
        )

    await request.app.redis.delete_measurement_state(measurement_uuid)
    return {"uuid": measurement_uuid, "action": "canceled"}


@router.get(
    "/{measurement_uuid}/{agent_uuid}",
    response_model=MeasurementsResultsResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get measurement results",
)
async def get_measurement_results(
    request: Request,
    measurement_uuid: UUID,
    agent_uuid: UUID,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    username: str = Depends(authenticate),
):
    """Get measurement results."""

    session = get_session(request.app.settings)

    measurement_info = await DatabaseMeasurements(
        session, request.app.settings, request.app.logger
    ).get(username, measurement_uuid)
    if measurement_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    table_name = (
        request.app.settings.DATABASE_NAME
        + "."
        + DatabaseMeasurementResults.forge_table_name(measurement_uuid, agent_uuid)
    )

    agent_specific_info = await DatabaseAgentsSpecific(
        session, request.app.settings, request.app.logger
    ).get(measurement_uuid, agent_uuid)

    if agent_specific_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"The agent `{agent_uuid}` "
                f"did not participate to measurement `{measurement_uuid}`"
            ),
        )

    if agent_specific_info["state"] != "finished":
        raise HTTPException(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            detail=(
                f"The agent `{agent_uuid}` "
                f"has not finished the measurement `{measurement_uuid}`"
            ),
        )

    database = DatabaseMeasurementResults(
        session, request.app.settings, table_name, request.app.logger
    )

    is_table_exists = await database.is_exists()
    if not is_table_exists:
        return {"count": 0, "next": None, "previous": None, "results": []}

    querier = DatabasePagination(database, request, offset, limit)
    return await querier.query()
