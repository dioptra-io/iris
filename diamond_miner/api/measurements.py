"""Measurements operations."""

from datetime import datetime
from diamond_miner.api import logger
from diamond_miner.api.settings import APISettings
from diamond_miner.commons.storage import Storage
from diamond_miner.worker.hooks import hook
from fastapi import APIRouter, BackgroundTasks, Request, status, HTTPException
from pydantic import BaseModel
from uuid import uuid4


router = APIRouter()
settings = APISettings()
storage = Storage()


def measurement_formater(uuid, ongoing_measurements):
    if uuid in ongoing_measurements:
        status = "ongoing"
    else:
        status = "finished"
    return {"uuid": uuid, "status": status}


@router.get("/")
async def get_measurements(request: Request):
    """Get global measurements information."""
    all_measurements = await request.app.redis.get_measurements()
    ongoing_measurements = await storage.get_ongoing_measurements()

    measurements = []
    for measurement in all_measurements:
        uuid = measurement.decode("utf-8")
        measurements.append(measurement_formater(uuid, ongoing_measurements))

    return {
        "count": len(measurements),
        "results": measurements,
    }


@router.get("/{uuid}")
async def get_measurement_by_uuid(request: Request, uuid: str):
    """Get measurement results by uuid."""
    all_measurements = await request.app.redis.get_measurements()
    ongoing_measurements = await storage.get_ongoing_measurements()

    for measurement in all_measurements:
        if measurement.decode("utf-8") == uuid:
            return measurement_formater(uuid, ongoing_measurements)
    raise HTTPException(status_code=404, detail="Measurement not found")


class Measurement(BaseModel):
    target_file_key: str
    protocol: str
    destination_port: int
    min_ttl: int
    max_ttl: int


async def publish_measurement(redis, measurement_uuid, timestamp, agents, parameters):
    """Launch a measurement procedure on each available agents."""
    await redis.register_measurement(measurement_uuid)
    try:
        await storage.create_bucket(bucket=measurement_uuid)
    except Exception:
        logger.error(f"Impossible to create bucket {measurement_uuid}")
        return

    await redis.publish(
        "request:all",
        {
            "measurement_uuid": measurement_uuid,
            "measurement_tool": "diamond_miner",
            "timestamp": timestamp,
            "round": 1,
            "parameters": dict(parameters),
        },
    )

    hook.send(measurement_uuid, timestamp, agents, dict(parameters))


@router.post("/", status_code=status.HTTP_201_CREATED)
async def post_measurement(
    request: Request, background_tasks: BackgroundTasks, measurement: Measurement
):
    """Launch a measurement."""
    try:
        await storage.get_file(
            settings.AWS_S3_TARGETS_BUCKET_NAME, measurement.target_file_key
        )
    except Exception:
        raise HTTPException(status_code=404, detail="File object not found")

    agents = await request.app.redis.agents_info()
    agents = [client for client, state in agents if state is True]
    if not agents:
        raise HTTPException(status_code=404, detail="No client available")

    measurement_uuid = str(uuid4())
    timestamp = datetime.timestamp(datetime.now())

    background_tasks.add_task(
        publish_measurement,
        request.app.redis,
        measurement_uuid,
        timestamp,
        agents,
        measurement,
    )

    return {"measurement": measurement_uuid}
