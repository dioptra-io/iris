"""Measurements operations."""

import aioboto3

from diamond_miner.api import logger
from diamond_miner.commons.settings import Settings
from diamond_miner.worker.handlers import handler
from fastapi import APIRouter, BackgroundTasks, Request, status, HTTPException
from uuid import uuid4


router = APIRouter()
settings = Settings()

aws_settings = {
    "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
    "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
    "endpoint_url": settings.AWS_S3_HOST,
    "region_name": settings.AWS_REGION_NAME,
}

# TODO Refactor that properly
tasks = {}


async def publish_measurement(redis, measurement_uuid, agents, target_file_key):
    """Launch a measurement procedure on each available agents."""
    global tasks

    tasks[measurement_uuid] = False

    async with aioboto3.client("s3", **aws_settings) as s3:
        try:
            await s3.create_bucket(Bucket=measurement_uuid)
        except Exception:
            logger.error(f"Impossible to create bucket {measurement_uuid}")
            return

    await redis.publish(
        "request:all",
        {"round": 1, "measurement_uuid": measurement_uuid, "targets": target_file_key},
    )

    handler.send(measurement_uuid, agents)

    tasks[measurement_uuid] = True


def task_formater(status):
    if status:
        return "finished"
    return "working"


@router.get("/")
def get_measurements():
    """Get global measurements information."""
    return [
        {"uuid": uuid, "status": task_formater(status)}
        for uuid, status in tasks.items()
    ]


@router.get("/{uuid}")
def get_measurement_by_uuid(uuid: str):
    """Get measurement results by uuid."""
    status = tasks.get(uuid)
    if status is None:
        raise HTTPException(status_code=404, detail="Measurement not found")
    return {"uuid": uuid, "status": task_formater(status)}


@router.post("/", status_code=status.HTTP_201_CREATED)
async def post_measurement(
    request: Request, background_tasks: BackgroundTasks, target_file_key: str
):
    """Launch a measurement."""
    async with aioboto3.client("s3", **aws_settings) as s3:
        try:
            file_object = await s3.get_object(
                Bucket=settings.AWS_S3_TARGETS_BUCKET_NAME, Key=target_file_key
            )
            async with file_object["Body"] as stream:
                await stream.read()
        except Exception:
            raise HTTPException(status_code=404, detail="File object not found")

        await s3.close()

    agents = await request.app.redis.agents_info()
    agents = [client for client, state in agents if state is True]
    if not agents:
        raise HTTPException(status_code=404, detail="No client available")

    measurement_uuid = str(uuid4())
    background_tasks.add_task(
        publish_measurement,
        request.app.redis,
        measurement_uuid,
        agents,
        target_file_key,
    )

    return {"measurement": measurement_uuid}
