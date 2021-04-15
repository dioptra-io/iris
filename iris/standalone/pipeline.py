import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiofiles

from iris import __version__
from iris.agent.measurements import measurement
from iris.agent.settings import AgentSettings
from iris.api.schemas import Tool, ToolParameters
from iris.commons.database import Database, DatabaseMeasurementResults, get_session
from iris.commons.dataclasses import ParametersDataclass
from iris.commons.utils import get_own_ip_address
from iris.standalone.storage import LocalStorage
from iris.worker.pipeline import default_pipeline
from iris.worker.settings import WorkerSettings


def create_request(
    settings: AgentSettings,
    tool: Tool,
    targets_file: Path,
    probes_filename,
    probing_rate: int,
    tool_parameters: ToolParameters,
    measurement_uuid: str,
    agent_uuid: str,
    round_number: int,
) -> dict:
    tool_parameters = tool_parameters.dict()
    tool_parameters["protocol"] = tool_parameters["protocol"].value
    return {
        "measurement_uuid": measurement_uuid,
        "username": "standalone",
        "round": round_number,
        "probes": probes_filename,
        "parameters": {
            "version": __version__,
            "hostname": "",
            "ip_address": get_own_ip_address(),
            "min_ttl": settings.AGENT_MIN_TTL,
            "max_probing_rate": settings.AGENT_MAX_PROBING_RATE,
            "targets_file": targets_file.name,
            "tool": tool,
            "probing_rate": probing_rate,
            "tool_parameters": tool_parameters,
            "tags": ["standalone"],  # TODO Be able to change the tags with the CLI
            "measurement_uuid": measurement_uuid,
            "user": "standalone",
            "start_time": "",
            "agent_uuid": agent_uuid,
        },
    }


async def pipeline(
    tool: Tool,
    prefixes: list,
    probing_rate: int,
    tool_parameters: ToolParameters,
    logger,
) -> str:
    """Measurement pipeline."""
    # Get all settings
    agent_settings = AgentSettings()
    worker_settings = WorkerSettings()

    # Create the database if not exists
    session = get_session(agent_settings)
    await Database(session, agent_settings, logger=logger).create_database(
        agent_settings.DATABASE_NAME
    )

    # Create a targets file
    targets_file: Path = agent_settings.AGENT_TARGETS_DIR_PATH / "prefixes.txt"
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

    measurement_uuid: str = str(uuid.uuid4())
    shuffled_next_round_csv_filepath: Optional[str] = None

    start_time = datetime.now()
    for round_number in range(1, tool_parameters.max_round + 1):
        request: dict = create_request(
            agent_settings,
            tool,
            targets_file,
            shuffled_next_round_csv_filepath,
            probing_rate,
            tool_parameters,
            measurement_uuid,
            agent_settings.AGENT_UUID,
            round_number,
        )
        results_filename: str = await measurement(
            agent_settings, request, storage, logger
        )
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
            break

    return {
        "measurement_uuid": measurement_uuid,
        "agent_uuid": agent_settings.AGENT_UUID,
        "database_name": agent_settings.DATABASE_NAME,
        "table_name": DatabaseMeasurementResults.forge_table_name(
            measurement_uuid, agent_settings.AGENT_UUID
        ),
        "n_rounds": round_number,
        "start_time": start_time,
        "end_time": datetime.now(),
    }
