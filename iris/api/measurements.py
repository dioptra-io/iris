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
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    username: str = Depends(authenticate),
):
    """Get all measurements."""
    session = get_session(request.app.settings)
    database = DatabaseMeasurements(session, request.app.settings, request.app.logger)

    querier = DatabasePagination(database, request, offset, limit)
    output = await querier.query(user=username)

    # Refine measurements output
    measurements = []
    for measurement in output["results"]:
        state = await request.app.redis.get_measurement_state(measurement["uuid"])
        measurements.append(
            {
                "uuid": measurement["uuid"],
                "state": "finished" if state is None else state,
                "targets_file_key": measurement["targets_file_key"],
                "tags": measurement["tags"],
                "start_time": measurement["start_time"],
                "end_time": measurement["end_time"],
            }
        )

    output["results"] = measurements

    return output


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
            "targets_file_key": "prefixes.txt",
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 30,
        },
    ),
    username: str = Depends(authenticate),
):
    """Request a measurement."""
    parameters = dict(measurement)

    session = get_session(request.app.settings)
    users_database = DatabaseUsers(session, request.app.settings, request.app.logger)

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
            measurement.targets_file_key,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="File object not found"
        )

    # Get all connected agents
    active_agents = await request.app.redis.get_agents(state=False, parameters=False)
    active_agents = [agent["uuid"] for agent in active_agents]

    # Filter out by `agents` key input if provided
    if measurement.agents:
        measurement.agents = list(measurement.agents)

        agents = {}
        for agent in measurement.agents:
            agent_uuid = str(agent.uuid)
            if agent_uuid not in active_agents:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found"
                )
            agents[agent_uuid] = {
                "targets_file_key": agent.targets_file_key,
                "min_ttl": agent.min_ttl,
                "max_ttl": agent.max_ttl,
                "probing_rate": agent.probing_rate,
                "max_round": agent.max_round,
            }
    else:
        agents = {uuid: {} for uuid in active_agents}
    del parameters["agents"]

    # Add mesurement metadata
    parameters["measurement_uuid"] = str(uuid4())
    parameters["user"] = username
    parameters["start_time"] = datetime.timestamp(datetime.now())

    # launch a measurement procedure on the worker.
    hook.send(agents, parameters)

    return {"uuid": parameters["measurement_uuid"]}


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

        if agent_specific["probing_rate"] is not None:
            specific_probing_rate = agent_specific["probing_rate"]
        else:
            specific_probing_rate = agent_info["probing_rate"]

        agents.append(
            {
                "uuid": agent_specific["uuid"],
                "state": agent_specific["state"],
                "specific": {
                    "targets_file_key": agent_specific["targets_file_key"],
                    "min_ttl": agent_specific["min_ttl"],
                    "max_ttl": agent_specific["max_ttl"],
                    "probing_rate": specific_probing_rate,
                    "max_round": agent_specific["max_round"],
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
