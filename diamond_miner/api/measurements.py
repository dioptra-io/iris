"""Measurements operations."""

import ipaddress

from aioch import Client
from datetime import datetime
from diamond_miner.api import logger
from diamond_miner.api.settings import APISettings
from diamond_miner.commons.database import Database
from diamond_miner.commons.storage import Storage
from diamond_miner.worker.hooks import hook
from fastapi import APIRouter, BackgroundTasks, Request, status, HTTPException
from pydantic import BaseModel
from uuid import uuid4


router = APIRouter()
settings = APISettings()
storage = Storage()


async def measurement_formater_summary(uuid, ongoing_measurements):
    measurement = {"uuid": uuid}
    if uuid in ongoing_measurements:
        measurement["status"] = "ongoing"
    else:
        measurement["status"] = "finished"
    return measurement


def packet_formater(row):
    return {
        "source_ip": ipaddress.ip_address(row[0]),
        "destination_prefix": ipaddress.ip_address(row[1]),
        "destination_ip": ipaddress.ip_address(row[2]),
        "reply_ip": ipaddress.ip_address(row[3]),
        "protocol": row[4],
        "source_port": row[5],
        "destination_port": row[6],
        "ttl": row[7],
        "type": row[8],
        "code": row[9],
        "rtt": row[10],
        "reply_ttl": row[11],
        "reply_size": row[12],
        "round": row[13],
        # "snapshot": row[14], # Not curently used
    }


async def measurement_formater_full(uuid, ongoing_measurements):
    """Format a measurement."""
    measurement = await measurement_formater_summary(uuid, ongoing_measurements)

    if measurement["status"] != "finished":
        return measurement

    client = Client(settings.API_DATABASE_HOST)
    response = await client.execute(f"SHOW TABLES FROM {settings.API_DATABASE_NAME}")
    all_tables = [table[0] for table in response]

    measurement_uuid = measurement["uuid"]

    tables, agents, date = [], [], ""
    for table in all_tables:
        parsed_table_name = Database.parse_table_name(table)
        if parsed_table_name["measurement_uuid"] != measurement_uuid:
            continue
        tables.append(table)
        agents.append(parsed_table_name["agent_uuid"])
        date = datetime.fromtimestamp(parsed_table_name["timestamp"])

    measurement["date"] = date
    measurement["agents"] = agents

    # TODO Pagination, filtering
    results = []
    for table in tables:
        response = await client.execute(
            f"SELECT * FROM {settings.API_DATABASE_NAME}.{table}"
        )
        results = results + [packet_formater(row) for row in response]

    results = sorted(
        results, key=lambda x: (x["source_ip"], x["destination_ip"], x["ttl"])
    )
    measurement["count"] = len(results)
    measurement["results"] = results

    return measurement


@router.get("/")
async def get_measurements(request: Request):
    """Get global measurements information."""
    all_measurements = await request.app.redis.get_measurements()
    ongoing_measurements = await storage.get_ongoing_measurements()

    measurements = []
    for measurement in all_measurements:
        uuid = measurement.decode("utf-8")
        measurements.append(
            await measurement_formater_summary(uuid, ongoing_measurements)
        )

    return {
        "count": len(measurements),
        "results": measurements,
    }


@router.get("/{uuid}")
async def get_measurement_by_uuid(
    request: Request, uuid: str,
):
    """Get measurement results by uuid."""
    all_measurements = await request.app.redis.get_measurements()
    ongoing_measurements = await storage.get_ongoing_measurements()

    for measurement in all_measurements:
        if measurement.decode("utf-8") == uuid:
            return await measurement_formater_full(uuid, ongoing_measurements)
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
