"""Measurements operations."""

from datetime import datetime
from fastapi import (
    APIRouter,
    Body,
    Depends,
    HTTPException,
    Query,
    Request,
    status,
)
from iris.api.pagination import DatabasePagination
from iris.api.security import authenticate
from iris.api.schemas import (
    ExceptionResponse,
    MeasurementInfoResponse,
    MeasurementsGetResponse,
    MeasurementsPostBody,
    MeasurementsPostResponse,
    MeasurementsDeleteResponse,
    MeasurementsResultsResponse,
)
from iris.api.settings import APISettings
from iris.commons.database import (
    get_session,
    DatabaseUsers,
    DatabaseMeasurementResults,
    DatabaseMeasurements,
    DatabaseAgents,
    DatabaseAgentsSpecific,
)
from iris.commons.storage import Storage
from iris.worker.hook import hook
from uuid import UUID, uuid4


router = APIRouter()
settings = APISettings()
storage = Storage()


@router.get("/", response_model=MeasurementsGetResponse, summary="Get all measurements")
async def get_measurements(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=0, le=200),
    username: str = Depends(authenticate),
):
    """Get all measurements."""
    session = get_session()
    database = DatabaseMeasurements(session)

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
                "full": measurement["full"],
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
            "targets_file_key": "test.txt",
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

    session = get_session()
    users_database = DatabaseUsers(session)

    user_info = await users_database.get(username)
    if not user_info["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Account inactive",
        )

    if measurement.full:
        # Full snapshot requested
        parameters["targets_file_key"] = None
        if not user_info["is_full_capable"]:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Full capabilities not allowed",
            )
    elif measurement.targets_file_key:
        # Targets based snapshot requested
        # Verify that the targets file exists on AWS S3
        try:
            await storage.get_file_no_retry(
                settings.AWS_S3_TARGETS_BUCKET_PREFIX + username,
                measurement.targets_file_key,
            )
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="File object not found"
            )
        parameters["full"] = False
    else:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Either `targets_file_key` or `full` key is necessary",
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
    request: Request, measurement_uuid: UUID, username: str = Depends(authenticate),
):
    """Get measurement information by uuid."""
    session = get_session()
    measurement = await DatabaseMeasurements(session).get(username, measurement_uuid)
    if measurement is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    state = await request.app.redis.get_measurement_state(measurement_uuid)
    measurement["state"] = "finished" if state is None else state

    # Remove unnecessary information
    del measurement["user"]
    del measurement["min_ttl"]
    del measurement["max_ttl"]
    del measurement["max_round"]

    agents_specific = await DatabaseAgentsSpecific(session).all(measurement["uuid"])

    agents = []
    for agent_specific in agents_specific:
        agent_info = await DatabaseAgents(session).get(agent_specific["uuid"])

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
    request: Request, measurement_uuid: UUID, username: str = Depends(authenticate),
):
    """Cancel a measurement."""
    session = get_session()
    measurement_info = await DatabaseMeasurements(session).get(
        username, measurement_uuid
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
    session = get_session()
    measurement_info = await DatabaseMeasurements(session).get(
        username, measurement_uuid
    )
    if measurement_info is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Measurement not found"
        )

    table_name = (
        settings.DATABASE_NAME
        + "."
        + DatabaseMeasurementResults.forge_table_name(measurement_uuid, agent_uuid)
    )

    agent_specific_info = await DatabaseAgentsSpecific(session).get(
        measurement_uuid, agent_uuid
    )

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

    database = DatabaseMeasurementResults(session, table_name)

    is_table_exists = await database.is_exists()
    if not is_table_exists:
        return {"count": 0, "next": None, "previous": None, "results": []}

    querier = DatabasePagination(database, request, offset, limit)
    return await querier.query()
