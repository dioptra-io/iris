import logging
import uuid
from pathlib import Path

import aiofiles

from iris import __version__
from iris.agent.measurements import measurement
from iris.agent.settings import AgentSettings
from iris.api.schemas import ToolParameters
from iris.commons.database import Database, get_session
from iris.commons.dataclasses import ParametersDataclass
from iris.commons.utils import get_own_ip_address
from iris.standalone import Tool
from iris.standalone.storage import LocalStorage
from iris.worker.pipeline import default_pipeline
from iris.worker.settings import WorkerSettings


def create_logger(level):
    formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger("standalone")
    logger.setLevel(level)
    logger.addHandler(stream_handler)

    return logger


def create_request(
    tool: Tool,
    targets_file: Path,
    probes_filename,
    probing_rate: int,
    tool_parameters: ToolParameters,
    measurement_uuid: str,
    agent_uuid: str,
    round_number: int,
):
    return {
        "measurement_uuid": measurement_uuid,
        "username": "standalone",
        "round": round_number,
        "probes": probes_filename,
        "parameters": {
            "version": __version__,
            "hostname": "",
            "ip_address": get_own_ip_address(),
            "probing_rate": probing_rate,
            "targets_file": targets_file.name,
            "tool": tool,
            "tool_parameters": tool_parameters.dict(),
            "tags": ["standalone"],
            "measurement_uuid": measurement_uuid,
            "user": "standalone",
            "start_time": "",
            "agent_uuid": agent_uuid,
        },
    }


async def pipeline(
    tool: Tool, prefixes: list, probing_rate: int, tool_parameters: ToolParameters
):
    """Measurement pipeline."""
    # Create the logger
    logger = create_logger(logging.ERROR)

    # Get all settings
    agent_settings = AgentSettings()
    worker_settings = WorkerSettings()

    # Create the database if not exists
    session = get_session(agent_settings)
    await Database(session, agent_settings, logger=logger).create_database(
        agent_settings.DATABASE_NAME
    )

    # Create a targets file
    targets_file = agent_settings.AGENT_TARGETS_DIR_PATH / "prefixes.txt"
    async with aiofiles.open(targets_file, mode="w") as fd:
        for prefix in prefixes:
            await fd.write(prefix)

    # Copy the target file to the local storage
    storage = LocalStorage(agent_settings.AGENT_TARGETS_DIR_PATH / "local_storage")
    await storage.upload_file(
        agent_settings.AWS_S3_TARGETS_BUCKET_PREFIX + "standalone",
        targets_file.name,
        targets_file,
    )

    measurement_uuid = str(uuid.uuid4())
    round_number: int = 1
    shuffled_next_round_csv_filepath: int = None

    while True:
        request = create_request(
            tool,
            targets_file,
            shuffled_next_round_csv_filepath,
            probing_rate,
            tool_parameters,
            measurement_uuid,
            agent_settings.AGENT_UUID,
            round_number,
        )
        results_filename = await measurement(agent_settings, request, storage, logger)
        shuffled_next_round_csv_filepath = await default_pipeline(
            worker_settings,
            ParametersDataclass.from_request(request),
            results_filename,
            storage,
            logger,
        )

        if shuffled_next_round_csv_filepath is None:
            if not worker_settings.WORKER_DEBUG_MODE:
                logger.info("Removing local measurement directory")
                try:
                    await aiofiles.os.rmdir(
                        worker_settings.WORKER_RESULTS_DIR_PATH / measurement_uuid
                    )
                except OSError:
                    logger.error("Impossible to remove local measurement directory")
            return measurement_uuid

        round_number = round_number + 1
