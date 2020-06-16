"""Measurement interface."""

import aioboto3
import aiofiles
import asyncio
import random
import json

from aiofiles import os as aios
from diamond_miner.commons.settings import Settings
from pathlib import Path

settings = Settings()

aws_settings = {
    "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
    "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
    "endpoint_url": settings.AWS_S3_HOST,
    "region_name": settings.AWS_REGION_NAME,
}


# async def execute():
#     """Execute measurement with Diamond-Miner."""
#     cmd = (
#         settings.D_MINER_EXECUTABLE_PATH
#         + " -o "
#         + ofile
#         + " -r "
#         + str(options.probing_rate)
#         + " --buffer-sniffer-size="
#         + str(options.buffer_sniffer_size)
#         + " -d "
#         + str(options.n_destinations_24)
#         + " -i "
#         + str(options.inf_born)
#         + " -s "
#         + str(options.sup_born)
#         + " -p "
#         + str(options.proto)
#         + " --dport="
#         + str(options.dport)
#         + " --min-ttl="
#         + str(options.min_ttl)
#         + " --max-ttl="
#         + str(options.max_ttl)
#         + " --record-timestamp "
#         + " --start-time-log-file="
#         + start_time_log_file
#     )

#     proc = await asyncio.create_subprocess_shell(
#         cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
#     )

#     stdout, stderr = await proc.communicate()


# Measurement output
# await redis.set(
#     f"{measuremement_uuid}:{redis.uuid}",
#     json.dumps(
#         {
#             "message": "response_measurement",
#             "measurement_uuid": measuremement_uuid,
#             "results": value,
#             "state": "success",
#             "reason": None,
#         }
#     ),
# )


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
    async with aioboto3.client("s3", **aws_settings) as s3:
        with Path(result_filename).open("rb") as fin:
            await s3.upload_fileobj(fin, measuremement_uuid, result_filename)

    # Remove local result file
    await aios.remove(result_filename)

    # Unlock the client state
    await redis.set(f"state:{redis.uuid}", 1)
