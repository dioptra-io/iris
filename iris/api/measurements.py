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
from iris.commons.database import (
    DatabaseMeasurementResults,
    DatabaseMeasurements,
    DatabaseAgents,
    DatabaseAgentsInMeasurements,
)
from iris.commons.storage import Storage
from iris.worker.hooks import hook
from uuid import uuid4


router = APIRouter()
settings = APISettings()
storage = Storage()


async def session_measurements():
    return DatabaseMeasurements(
        host=settings.DATABASE_HOST, table_name=settings.MEASUREMENTS_TABLE_NAME
    )


async def session_agents():
    return DatabaseAgents(
        host=settings.DATABASE_HOST, table_name=settings.AGENTS_TABLE_NAME
    )


async def session_agents_in_measurements():
    return DatabaseAgentsInMeasurements(
        host=settings.DATABASE_HOST,
        table_name=settings.AGENTS_IN_MEASUREMENTS_TABLE_NAME,
    )


async def measurement_formater_results(
    request, measurement_uuid, agent_uuid, offset, limit
):
    """Measurement results for an agent.
    Get the results from the database.
    """
    state = await request.app.redis.get_measurement_state(measurement_uuid)
    if state is not None:
        return {"count": 0, "next": None, "previous": None, "results": []}

    client = aioch.Client(settings.DATABASE_HOST)
    table_name = DatabaseMeasurementResults.forge_table_name(
        measurement_uuid, agent_uuid
    )
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
    "/", response_model=MeasurementsGetResponse, summary="Get all measurements",
)
async def get_measurements(
    request: Request,
    username: str = Depends(authenticate),
    session: DatabaseMeasurements = Depends(session_measurements),
):
    """Get all measurements."""
    all_measurements = await session.all(username)

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
    request: Request,
    measurement_uuid: str,
    username: str = Depends(authenticate),
    session_measurement: DatabaseMeasurements = Depends(session_measurements),
    session_agents: DatabaseAgents = Depends(session_agents),
    session_agents_in_measurements: DatabaseAgentsInMeasurements = Depends(
        session_agents_in_measurements
    ),
):
    """Get measurement information by uuid."""
    measurement = await session_measurement.get(username, measurement_uuid)
    if measurement is None:
        raise HTTPException(status_code=404, detail="Measurement not found")

    state = await request.app.redis.get_measurement_state(measurement_uuid)
    measurement["uuid"] = measurement_uuid
    measurement["state"] = "finished" if state is None else state

    del measurement["user"]

    agents_in_measurement = await session_agents_in_measurements.all(
        measurement["uuid"]
    )

    agents = []
    for agent in agents_in_measurement:
        agent_info = await session_agents.get(agent["uuid"])
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
    measurement_uuid: str,
    agent_uuid: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=0, le=200),
    username: str = Depends(authenticate),
    session: DatabaseMeasurements = Depends(session_measurements),
):
    """Get measurement results."""
    measurement_info = await session.get(username, measurement_uuid)
    if measurement_info is None:
        raise HTTPException(status_code=404, detail="Measurement not found")

    return await measurement_formater_results(
        request, measurement_uuid, agent_uuid, offset, limit
    )
