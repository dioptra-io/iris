import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import aiofiles

from iris import __version__
from iris.agent.measurements import measurement
from iris.agent.settings import AgentSettings
from iris.api.schemas import Tool, ToolParameters
from iris.commons.database import (
    Database,
    DatabaseAgents,
    DatabaseAgentsSpecific,
    DatabaseMeasurementResults,
    DatabaseMeasurements,
    get_session,
)
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
    round_number: int,
    start_time,
    tags: List[str],
) -> dict:
    tool_parameters = tool_parameters.dict()
    tool_parameters["n_flow_ids"] = 6 if tool == "diamond-miner" else 1
    print(tool_parameters["n_flow_ids"])
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
            "tags": tags,
            "measurement_uuid": measurement_uuid,
            "user": "standalone",
            "start_time": start_time,
            "agent_uuid": settings.AGENT_UUID,
        },
    }


async def register_measurement(dataclass, settings, logger):
    session = get_session(settings)
    database_measurements = DatabaseMeasurements(session, settings, logger=logger)
    await database_measurements.create_table()
    await database_measurements.register(
        {
            "measurement_uuid": dataclass.measurement_uuid,
            "user": "standalone",
            "tool": dataclass.tool,
            "tags": dataclass.tags,
            "start_time": dataclass.start_time,
            "end_time": None,
        }
    )


async def register_agent(dataclass, settings, logger):
    session = get_session(settings)
    database_agents = DatabaseAgents(session, settings, logger=logger)
    database_agents_specific = DatabaseAgentsSpecific(session, settings, logger=logger)

    is_already_present = await database_agents.get(dataclass.agent_uuid)
    if is_already_present is None:
        # Physical agent not present, registering
        await database_agents.register(
            dataclass.agent_uuid, dataclass.physical_parameters
        )
    else:
        # Already present, updating last used
        await database_agents.stamp_last_used(dataclass.agent_uuid)

    # Register agent in this measurement and specific information
    await database_agents_specific.register(dataclass)


async def stamp_measurement(dataclass, settings, logger):
    session = get_session(settings)
    database_measurements = DatabaseMeasurements(session, settings, logger=logger)
    await database_measurements.stamp_end_time("standalone", dataclass.measurement_uuid)


async def pipeline(
    tool: Tool,
    prefixes: list,
    probing_rate: int,
    tool_parameters: ToolParameters,
    tags: List[str],
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
    start_time = datetime.now()

    shuffled_next_round_csv_filepath: Optional[str] = None
    for round_number in range(1, tool_parameters.max_round + 1):
        request: dict = create_request(
            agent_settings,
            tool,
            targets_file,
            shuffled_next_round_csv_filepath,
            probing_rate,
            tool_parameters,
            measurement_uuid,
            round_number,
            start_time.timestamp(),
            tags,
        )

        dataclass = ParametersDataclass.from_request(request)

        # If it's the first round,
        # register the measurement and the agent to the database
        if round_number == 1:
            await register_measurement(dataclass, worker_settings, logger)
            await register_agent(dataclass, worker_settings, logger)

        # Perform the measurement
        results_filename: str = await measurement(
            agent_settings, request, storage, logger
        )

        # Compute the next round
        shuffled_next_round_csv_filepath = await default_pipeline(
            worker_settings,
            dataclass,
            results_filename,
            storage,
            logger,
        )

        # If the measurement is finished, clean the local files
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

    # Stamp the measurement
    await stamp_measurement(dataclass, worker_settings, logger)

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
