"""Measurements operations."""

import aioch

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
from iris.commons.database import DatabaseMeasurement, DatabaseAllMeasurements
from iris.commons.storage import Storage
from iris.worker.hooks import hook
from uuid import uuid4


router = APIRouter()
settings = APISettings()
storage = Storage()


async def session():
    return DatabaseAllMeasurements(
        host=settings.DATABASE_HOST, table_name=settings.MEASUREMENT_TABLE_NAME
    )


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

    client = aioch.Client(settings.DATABASE_HOST)
    table_name = DatabaseMeasurement.forge_table_name(measurement_uuid, agent_uuid)
    table_name = f"{settings.DATABASE_NAME}.{table_name}"

    response = await client.execute(f"EXISTS TABLE {table_name}")
    is_table_exists = bool(response[0][0])
    if not is_table_exists:
        raise HTTPException(
            status_code=404,
            detail=(
                f"The agent `{agent_uuid}` "
                f"did not participate to measurement `{measurement_uuid}`"
            ),
        )

    querier = MeasurementResults(request, client, table_name, offset, limit)
    return await querier.query()


@router.get(
    "/",
    response_model=MeasurementsGetResponse,
    summary="Get all measurements with the status",
)
async def get_measurements(
    request: Request,
    username: str = Depends(authenticate),
    session: DatabaseAllMeasurements = Depends(session),
):
    """Get all measurements with the status."""
    all_measurements = await session.all(username)

    measurements = []
    for measurement_uuid in all_measurements:
        measurements.append(
            await measurement_formater_summary(request.app.redis, measurement_uuid)
        )

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
    measurement_uuid: str,
    username: str = Depends(authenticate),
    session: DatabaseAllMeasurements = Depends(session),
):
    """Get measurement information by uuid."""
    measurement_info = await session.get(username, measurement_uuid)
    if measurement_info is None:
        raise HTTPException(status_code=404, detail="Measurement not found")

    measurement = await measurement_formater_summary(
        request.app.redis, measurement_uuid
    )

    measurement = {**measurement, **measurement_info}
    del measurement["user"]

    return measurement


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
    session: DatabaseAllMeasurements = Depends(session),
):
    """Get measurement results."""
    measurement_info = await session.get(username, measurement_uuid)
    if measurement_info is None:
        raise HTTPException(status_code=404, detail="Measurement not found")

    return await measurement_formater_results(
        request, measurement_uuid, agent_uuid, offset, limit
    )
