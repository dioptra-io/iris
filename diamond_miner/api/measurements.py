"""Measurements operations."""

import ipaddress

from aioch import Client
from datetime import datetime
from diamond_miner.api.settings import APISettings
from diamond_miner.commons.database import Database
from diamond_miner.commons.storage import Storage
from diamond_miner.worker.hooks import hook
from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Request, status
from pydantic import BaseModel
from uuid import uuid4


router = APIRouter()
settings = APISettings()
storage = Storage()


async def measurement_formater_summary(redis, uuid):
    measurement = {"uuid": uuid}
    state = await redis.get_measurement_state(uuid)
    if state is not None:
        measurement["status"] = state
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
        "ttl_check": row[8],  # implemented only in UDP
        "type": row[9],
        "code": row[10],
        "rtt": row[11],
        "reply_ttl": row[12],
        "reply_size": row[13],
        "round": row[14],
        # "snapshot": row[14], # Not curently used
    }


async def measurement_formater_full(redis, uuid):
    """Format a measurement."""
    measurement = await measurement_formater_summary(redis, uuid)

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

    measurements = []
    for measurement_uuid in all_measurements:
        measurements.append(
            await measurement_formater_summary(request.app.redis, measurement_uuid)
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
    for measurement_uuid in all_measurements:
        if measurement_uuid == uuid:
            return await measurement_formater_full(request.app.redis, uuid)
    raise HTTPException(status_code=404, detail="Measurement not found")


class Measurement(BaseModel):
    target_file_key: str
    protocol: str
    destination_port: int
    min_ttl: int
    max_ttl: int


async def publish_measurement(redis, agents, parameters):
    """Launch a measurement procedure on each available agents."""
    measurement_uuid = parameters["measurement_uuid"]
    await redis.register_measurement(measurement_uuid)
    hook.send(agents, parameters)


@router.post("/", status_code=status.HTTP_201_CREATED)
async def post_measurement(
    request: Request,
    background_tasks: BackgroundTasks,
    measurement: Measurement = Body(
        ...,
        example={
            "target_file_key": "test.txt",
            "protocol": "udp",
            "destination_port": 33434,
            "min_ttl": 2,
            "max_ttl": 30,
        },
    ),
):
    """Launch a measurement."""
    try:
        await storage.get_file(
            settings.AWS_S3_TARGETS_BUCKET_NAME, measurement.target_file_key
        )
    except Exception:
        raise HTTPException(status_code=404, detail="File object not found")

    agents = await request.app.redis.get_agents(state=False, parameters=False)
    agents = [agent["uuid"] for agent in agents]

    parameters = dict(measurement)
    parameters["measurement_uuid"] = str(uuid4())
    parameters["timestamp"] = datetime.timestamp(datetime.now())

    background_tasks.add_task(
        publish_measurement, request.app.redis, agents, parameters,
    )

    return {"measurement": parameters["measurement_uuid"]}
