"""Measurement interface."""

import aiofiles
import asyncio
import random
import json

from aiofiles import os as aios
from diamond_miner.agent.settings import AgentSettings
from diamond_miner.commons.storage import Storage
from pathlib import Path

settings = AgentSettings()
storage = Storage()


async def execute(parameters, ofile, start_time_log_file):
    """Execute measurement with Diamond-Miner."""
    cmd = (
        settings.AGENT_D_MINER_EXE_PATH
        + " -o "
        + ofile
        + " -r "
        + str(settings.AGENT_PROBING_RATE)
        + " --buffer-sniffer-size="
        + str(settings.AGENT_BUFFER_SNIFFER_SIZE)
        + " -d "
        + str(settings.AGENT_IPS_PER_SUBNET)
        + " -i "
        + str(settings.AGENT_INF_BORN)
        + " -s "
        + str(settings.AGENT_SUP_BORN)
        + " -p "
        + str(parameters["protocol"])
        + " --dport="
        + str(parameters["destination_port"])
        + " --min-ttl="
        + str(parameters["min_ttl"])
        + " --max-ttl="
        + str(parameters["max_ttl"])
        + " --record-timestamp "
        + " --start-time-log-file="
        + start_time_log_file
    )

    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()


async def measuremement(redis, request):
    """Conduct a measurement."""

    # Lock the client state
    await redis.set(f"state:{redis.uuid}", 0)

    measuremement_uuid = request["measurement_uuid"]
    round_number = request["round"]

    result_filename = f"{redis.uuid}_{round_number}.json"

    # Simulate a measurement
    value = random.randint(5, 10)
    await asyncio.sleep(value)

    async with aiofiles.open(f"{result_filename}", "w") as fin:
        await fin.write(json.dumps({"result": value}, indent=4))

    # # Upload result file into AWS S3
    with Path(result_filename).open("rb") as fin:
        await storage.upload_file(measuremement_uuid, result_filename, fin)

    # Remove local result file
    await aios.remove(result_filename)

    # Unlock the client state
    await redis.set(f"state:{redis.uuid}", 1)
