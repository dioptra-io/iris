import logging
import socket
import uuid
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
from clickhouse_driver import Client
from diamond_miner.queries.count_links import CountLinks
from diamond_miner.queries.count_nodes import CountNodesFromResults

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
    target_file: Path,
    probes_filename,
    username,
    probing_rate: int,
    tool_parameters: ToolParameters,
    measurement_uuid: str,
    round_number: int,
    start_time,
    tags: List[str],
) -> dict:
    tool_parameters = tool_parameters.dict()
    tool_parameters["n_flow_ids"] = 6 if tool == "diamond-miner" else 1
    return {
        "measurement_uuid": measurement_uuid,
        "username": username,
        "round": round_number,
        "probes": probes_filename,
        "parameters": {
            "version": __version__,
            "hostname": socket.gethostname(),
            "ip_address": get_own_ip_address(),
            "min_ttl": settings.AGENT_MIN_TTL,
            "max_probing_rate": settings.AGENT_MAX_PROBING_RATE,
            "target_file": target_file.name,
            "tool": tool,
            "probing_rate": probing_rate,
            "tool_parameters": tool_parameters,
            "tags": tags,
            "measurement_uuid": measurement_uuid,
            "user": username,
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
            "user": dataclass.user,
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
    await database_measurements.stamp_finished(
        dataclass.user, dataclass.measurement_uuid
    )
    await database_measurements.stamp_end_time(
        dataclass.user, dataclass.measurement_uuid
    )


async def stamp_agent(dataclass, settings, logger):
    session = get_session(settings)
    database_agents_specific = DatabaseAgentsSpecific(session, settings, logger=logger)
    await database_agents_specific.stamp_finished(
        dataclass.measurement_uuid, dataclass.agent_uuid
    )


async def pipeline(
    tool: Tool,
    prefixes: list,
    username: str,
    probing_rate: int,
    tool_parameters: ToolParameters,
    tags: List[str],
    logger,
) -> Dict:
    """Measurement pipeline."""
    # Silence the tzlocal timezone configuration warning
    warnings.filterwarnings("ignore", category=UserWarning, module="tzlocal")

    # Get all settings
    agent_settings = AgentSettings()
    if logger.level >= logging.INFO:
        agent_settings.AGENT_CARACAL_LOGGING_LEVEL = logging.WARNING
    worker_settings = WorkerSettings()

    measurement_uuid: str = str(uuid.uuid4())
    start_time = datetime.now()

    # Create the database if not exists
    session = get_session(agent_settings)
    await Database(session, agent_settings, logger=logger).create_database(
        agent_settings.DATABASE_NAME
    )

    # Create a target file
    target_file: Path = (
        agent_settings.AGENT_TARGETS_DIR_PATH
        / f"targets__{measurement_uuid}__{agent_settings.AGENT_UUID}.csv"
    )
    async with aiofiles.open(target_file, mode="w") as fd:
        for prefix in prefixes:
            await fd.write(prefix)

    # Copy the target file to the local storage
    storage = LocalStorage(agent_settings.AGENT_TARGETS_DIR_PATH / "local_storage")
    await storage.upload_file(
        agent_settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + username,
        target_file.name,
        target_file,
    )

    shuffled_next_round_csv_filepath: Optional[str] = None
    for round_number in range(1, tool_parameters.max_round + 1):
        request: dict = create_request(
            agent_settings,
            tool,
            target_file,
            shuffled_next_round_csv_filepath,
            username,
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

    # Stamp the agent
    await stamp_agent(dataclass, agent_settings, logger)

    # Stamp the measurement
    await stamp_measurement(dataclass, worker_settings, logger)

    # Compute distinct nodes/links
    client = Client(agent_settings.DATABASE_HOST)
    results_table_name = DatabaseMeasurementResults.forge_table_name(
        measurement_uuid, agent_settings.AGENT_UUID
    )
    links_table_name = "links__" + "__".join(results_table_name.split("__")[1:3])
    n_nodes = CountNodesFromResults().execute(
        client, agent_settings.DATABASE_NAME + "." + results_table_name
    )[0][0]
    n_links = CountLinks().execute(
        client, agent_settings.DATABASE_NAME + "." + links_table_name
    )[0][0]

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
        "n_nodes": n_nodes,
        "n_links": n_links,
    }
