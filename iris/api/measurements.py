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
from iris.api.results import MeasurementResults
from iris.api.security import authenticate
from iris.api.schemas import (
    ExceptionResponse,
    MeasurementInfoResponse,
    MeasurementsGetResponse,
    MeasurementsPostBody,
    MeasurementsPostResponse,
    MeasurementsResultsResponse,
)
from iris.api.settings import APISettings
from iris.commons.database import (
    get_session,
    DatabaseMeasurementResults,
    DatabaseMeasurements,
    DatabaseAgents,
    DatabaseAgentsInMeasurements,
)
from iris.commons.storage import Storage
from iris.worker.hooks import hook
from uuid import UUID, uuid4


router = APIRouter()
settings = APISettings()
storage = Storage()


@router.get(
    "/", response_model=MeasurementsGetResponse, summary="Get all measurements",
)
async def get_measurements(
    request: Request, username: str = Depends(authenticate),
):
    """Get all measurements."""
    session = get_session()
    all_measurements = await DatabaseMeasurements(session).all(username)

    measurements = []
    for measurement in all_measurements:
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

    # Sort measurements by `start_time`
    measurements.sort(key=lambda x: x["start_time"])

    return {"count": len(measurements), "results": measurements}


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

    if measurement.full:
        # Full snapshot requested
        parameters["targets_file_key"] = None
    elif measurement.targets_file_key:
        # Targets based snapshot requested
        # Verify that the targets file exists on AWS S3
        try:
            await storage.get_file(
                settings.AWS_S3_TARGETS_BUCKET_NAME, measurement.targets_file_key
            )
        except Exception:
            raise HTTPException(status_code=404, detail="File object not found")
        parameters["full"] = False
    else:
        raise HTTPException(
            status_code=422,
            detail="Either `targets_file_key` or `full` key is necessary",
        )

    # Get all connected agents
    agents = await request.app.redis.get_agents(state=False, parameters=False)
    agents = [agent["uuid"] for agent in agents]

    # Filter out by `agents` key input if provided
    if measurement.agents:
        measurement.agents = list(measurement.agents)
        for agent in measurement.agents:
            if agent not in agents:
                raise HTTPException(status_code=404, detail="Agent not found")
        agents = measurement.agents

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
        raise HTTPException(status_code=404, detail="Measurement not found")

    state = await request.app.redis.get_measurement_state(measurement_uuid)
    measurement["uuid"] = measurement_uuid
    measurement["state"] = "finished" if state is None else state

    del measurement["user"]

    agents_in_measurement = await DatabaseAgentsInMeasurements(session).all(
        measurement["uuid"]
    )

    agents = []
    for agent in agents_in_measurement:
        agent_info = await DatabaseAgents(session).get(agent["uuid"])
        agents.append(
            {
                "uuid": agent["uuid"],
                "state": agent["state"],
                "min_ttl": agent["min_ttl"],
                "max_ttl": agent["max_ttl"],
                "parameters": {
                    "version": agent_info["version"],
                    "hostname": agent_info["hostname"],
                    "ip_address": agent_info["ip_address"],
                    "probing_rate": agent_info["probing_rate"],
                },
            }
        )
    measurement["agents"] = agents

    return measurement


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
        raise HTTPException(status_code=404, detail="Measurement not found")

    table_name = DatabaseMeasurementResults.forge_table_name(
        measurement_uuid, agent_uuid
    )
    table_name = f"{settings.DATABASE_NAME}.{table_name}"

    agent_in_measurement_info = await DatabaseAgentsInMeasurements(session).get(
        measurement_uuid, agent_uuid
    )

    if agent_in_measurement_info is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"The agent `{agent_uuid}` "
                f"did not participate to measurement `{measurement_uuid}`"
            ),
        )

    if agent_in_measurement_info["state"] != "finished":
        return {"count": 0, "next": None, "previous": None, "results": []}

    querier = MeasurementResults(request, session, table_name, offset, limit)
    return await querier.query()
