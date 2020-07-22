"""Measurements operations."""

from aioch import Client
from datetime import datetime
from fastapi import (
    APIRouter,
    BackgroundTasks,
    Body,
    Depends,
    HTTPException,
    Query,
    Request,
    status,
)
from iris.api.database import MeasurementResults, get_table_name, get_agents_and_date
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
from iris.commons.storage import Storage
from iris.worker.hooks import hook
from uuid import uuid4


router = APIRouter()
settings = APISettings()
storage = Storage()


async def measurement_formater_summary(redis, uuid):
    """Summary of a measurements.
    Only display the uuid of the measurement and the state from Redis.
    `finished` if no state.
    """
    measurement = {"uuid": uuid}
    state = await redis.get_measurement_state(uuid)
    if state is not None:
        measurement["status"] = state
    else:
        measurement["status"] = "finished"
    return measurement


async def measurement_formater_info(redis, uuid):
    """Measurement information.
    Get the state from Redis (`finished` if no state)
    The other information is derived from the database.
    """
    measurement = await measurement_formater_summary(redis, uuid)

    if measurement["status"] != "finished":
        measurement["agents"] = None
        measurement["date"] = None
        return measurement

    client = Client(settings.API_DATABASE_HOST)
    measurement["agents"], measurement["date"] = await get_agents_and_date(client, uuid)

    return measurement


async def measurement_formater_results(
    request, measurement_uuid, agent_uuid, offset, limit
):
    """Measurement results for an agent.
    Get the results from the database only.
    """
    measurement = await measurement_formater_summary(
        request.app.redis, measurement_uuid
    )
    if measurement["status"] != "finished":
        return {"count": 0, "results": []}

    client = Client(settings.API_DATABASE_HOST)

    table_name = await get_table_name(client, measurement_uuid, agent_uuid)
    if table_name is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"The agent `{agent_uuid}` "
                f"did not participate to measurement `{measurement_uuid}`",
            ),
        )

    querier = MeasurementResults(request, client, table_name, offset, limit)
    return await querier.query()


@router.get(
    "/",
    response_model=MeasurementsGetResponse,
    summary="Get all measurements with the status",
)
async def get_measurements(request: Request, username: str = Depends(authenticate)):
    """Get all measurements with the status."""
    all_measurements = await request.app.redis.get_measurements()

    measurements = []
    for measurement_uuid in all_measurements:
        measurements.append(
            await measurement_formater_summary(request.app.redis, measurement_uuid)
        )

    return {"count": len(measurements), "results": measurements}


async def publish_measurement(redis, agents, parameters):
    """Launch a measurement procedure on each available agents."""
    measurement_uuid = parameters["measurement_uuid"]
    await redis.register_measurement(measurement_uuid)
    hook.send(agents, parameters)


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    response_model=MeasurementsPostResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Request a measurement",
)
async def post_measurement(
    request: Request,
    background_tasks: BackgroundTasks,
    measurement: MeasurementsPostBody = Body(
        ...,
        example={
            "target_file_key": "test.txt",
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 30,
        },
    ),
    username: str = Depends(authenticate),
):
    """Request a measurement."""
    try:
        await storage.get_file(
            settings.AWS_S3_TARGETS_BUCKET_NAME, measurement.target_file_key
        )
    except Exception:
        raise HTTPException(status_code=404, detail="File object not found")

    agents = await request.app.redis.get_agents(state=False, parameters=False)
    agents = [agent["uuid"] for agent in agents]

    if measurement.agents:
        measurement.agents = list(measurement.agents)
        for agent in measurement.agents:
            if agent not in agents:
                raise HTTPException(status_code=404, detail="Agent not found")

    parameters = dict(measurement)
    parameters["measurement_uuid"] = str(uuid4())
    parameters["timestamp"] = datetime.timestamp(datetime.now())

    background_tasks.add_task(
        publish_measurement, request.app.redis, agents, parameters
    )

    return {"uuid": parameters["measurement_uuid"]}


@router.get(
    "/{measurement_uuid}",
    response_model=MeasurementInfoResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get measurement information by uuid",
)
async def get_measurement_by_uuid(
    request: Request, measurement_uuid: str, username: str = Depends(authenticate)
):
    """Get measurement information by uuid."""
    all_measurements = await request.app.redis.get_measurements()
    for measurement_uuid in all_measurements:
        if measurement_uuid == measurement_uuid:
            return await measurement_formater_info(request.app.redis, measurement_uuid)
    raise HTTPException(status_code=404, detail="Measurement not found")


@router.get(
    "/{measurement_uuid}/{agent_uuid}",
    response_model=MeasurementsResultsResponse,
    responses={404: {"model": ExceptionResponse}},
    summary="Get measurement results",
)
async def get_measurement_results(
    request: Request,
    measurement_uuid: str,
    agent_uuid: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    username: str = Depends(authenticate),
):
    """Get measurement results."""
    all_measurements = await request.app.redis.get_measurements()
    for measurement_uuid in all_measurements:
        if measurement_uuid == measurement_uuid:
            return await measurement_formater_results(
                request, measurement_uuid, agent_uuid, offset, limit
            )
    raise HTTPException(status_code=404, detail="Measurement not found")
