"""Measurement interface."""

from aiofiles import os as aios
from iris.agent import logger
from iris.agent.prober import probe
from iris.agent.settings import AgentSettings
from iris.commons.storage import Storage
from pathlib import Path


settings = AgentSettings()
storage = Storage()


async def measuremement(uuid, request):
    """Conduct a measurement."""
    # Lock the client state
    measurement_uuid = request["measurement_uuid"]
    round_number = request["round"]

    result_filename = f"{uuid}_results_{round_number}.pcap"
    result_filepath = str(settings.AGENT_RESULTS_DIR / result_filename)
    starttime_filename = f"{uuid}_starttime_{round_number}.log"
    starttime_filepath = str(settings.AGENT_RESULTS_DIR / starttime_filename)

    if round_number == 1:
        # Download target file locally
        target_filename = request["parameters"]["target_file_key"]
        target_filepath = str(settings.AGENT_TARGETS_DIR / target_filename)
        await storage.download_file(
            settings.AWS_S3_TARGETS_BUCKET_NAME, target_filename, target_filepath
        )
        csv_filepath = None
    else:
        # Download CSV probe file locally
        target_filepath = None
        csv_filename = request["parameters"]["csv_probe_file"]
        csv_filepath = str(settings.AGENT_TARGETS_DIR / csv_filename)
        await storage.download_file(measurement_uuid, csv_filename, csv_filepath)

    # Diamond-Miner measurement
    await probe(
        request,
        result_filepath,
        starttime_filepath,
        target_filepath=target_filepath,
        csv_filepath=csv_filepath,
    )

    # Upload result file & start time log file into AWS S3
    with Path(result_filepath).open("rb") as fin:
        await storage.upload_file(measurement_uuid, result_filename, fin)
    with Path(starttime_filepath).open("rb") as fin:
        await storage.upload_file(measurement_uuid, starttime_filename, fin)

    # Remove local result file
    await aios.remove(result_filepath)
    await aios.remove(starttime_filepath)

    # Remove local target files or CSV probe file
    if target_filepath is not None:
        await aios.remove(target_filepath)
    if csv_filepath is not None:
        await aios.remove(csv_filepath)

    # Remove remote CSV probe file on AWS S3 if needed
    if csv_filepath is not None:
        logger.info("Remove CSV probe file from AWS S3")
        response = await storage.delete_file_no_check(measurement_uuid, csv_filename)
        if response["ResponseMetadata"]["HTTPStatusCode"] != 204:
            logger.error(f"Impossible to remove result file `{csv_filename}`")
