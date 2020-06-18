"""Measurement interface."""

from aiofiles import os as aios
from diamond_miner.agent.prober import probe
from diamond_miner.agent.settings import AgentSettings
from diamond_miner.commons.storage import Storage
from pathlib import Path


settings = AgentSettings()
storage = Storage()


async def measuremement(redis, request):
    """Conduct a measurement."""
    # Lock the client state
    await redis.set(f"state:{redis.uuid}", 0)

    measuremement_uuid = request["measurement_uuid"]
    round_number = request["round"]

    result_filename = f"{redis.uuid}_results_{round_number}.pcap"
    result_filepath = str(settings.AGENT_RESULTS_DIR / result_filename)
    starttime_filename = f"{redis.uuid}_starttime_{round_number}.log"
    starttime_filepath = str(settings.AGENT_RESULTS_DIR / starttime_filename)

    # Download target file locally
    if round_number == 1:
        target_filename = request["parameters"]["target_file_key"]
        target_filepath = str(settings.AGENT_TARGETS_DIR / target_filename)
        await storage.download_file(
            settings.AWS_S3_TARGETS_BUCKET_NAME, target_filename, target_filepath,
        )
    else:
        target_filepath = None
        # TODO CSV file for round >= 2

    # Diamond-Miner measurement
    await probe(
        request, result_filepath, starttime_filepath, target_filepath=target_filepath
    )

    # Upload result file & start time log file into AWS S3
    with Path(result_filepath).open("rb") as fin:
        await storage.upload_file(measuremement_uuid, result_filename, fin)
    with Path(starttime_filepath).open("rb") as fin:
        await storage.upload_file(measuremement_uuid, starttime_filename, fin)

    # Remove local result file
    await aios.remove(result_filepath)
    await aios.remove(starttime_filepath)

    # Unlock the client state
    await redis.set(f"state:{redis.uuid}", 1)
