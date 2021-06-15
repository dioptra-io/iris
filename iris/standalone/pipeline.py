import logging
import socket
import uuid
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
from diamond_miner.queries import CountLinks, CountNodesFromResults, results_table

from iris import __version__
from iris.agent.measurements import measurement
from iris.agent.settings import AgentSettings
from iris.agent.ttl import find_exit_ttl
from iris.api.schemas import Tool, ToolParameters
from iris.commons.database import Agents, Database, Measurements
from iris.commons.dataclasses import ParametersDataclass
from iris.commons.round import Round
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
    round: Round,
    start_time,
    tags: List[str],
) -> dict:
    tool_parameters = tool_parameters.dict()
    tool_parameters["n_flow_ids"] = 6 if tool == "diamond-miner" else 1
    tool_parameters["global_min_ttl"] = 1
    tool_parameters["global_max_ttl"] = 32
    return {
        "measurement_uuid": measurement_uuid,
        "username": username,
        "round": round.encode(),
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
    database_measurements = Measurements(settings, logger)
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
    database_agents = Agents(settings, logger)

    # Create `agents` and `agents_specific` tables
    await database_agents.create_table()

    # Register agent in this measurement and specific information
    await database_agents.register(dataclass)


async def stamp_measurement(dataclass, settings, logger):
    database_measurements = Measurements(settings, logger)
    await database_measurements.stamp_finished(
        dataclass.user, dataclass.measurement_uuid
    )
    await database_measurements.stamp_end_time(
        dataclass.user, dataclass.measurement_uuid
    )


async def stamp_agent(dataclass, settings, logger):
    database_agents = Agents(settings, logger)
    await database_agents.stamp_finished(
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

    # Find min TTL automatically
    if agent_settings.AGENT_MIN_TTL < 0:
        agent_settings.AGENT_MIN_TTL = find_exit_ttl(
            logger, agent_settings.AGENT_MIN_TTL_FIND_TARGET, min_ttl=2
        )

    # Create the database if not exists
    await Database(agent_settings, logger).create_database()

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
    round = Round(1, worker_settings.WORKER_ROUND_1_SLIDING_WINDOW, 0)
    n_rounds = 0
    while round.number <= tool_parameters.max_round:
        n_rounds = round.number
        request: dict = create_request(
            agent_settings,
            tool,
            target_file,
            shuffled_next_round_csv_filepath,
            username,
            probing_rate,
            tool_parameters,
            measurement_uuid,
            round,
            start_time.timestamp(),
            tags,
        )

        dataclass = ParametersDataclass.from_request(request)

        # If it's the first round,
        # register the measurement and the agent to the database
        if round.number == 1:
            await register_measurement(dataclass, worker_settings, logger)
            await register_agent(dataclass, worker_settings, logger)

        # Perform the measurement
        results_filename: str = await measurement(
            agent_settings, request, storage, logger
        )

        # Compute the next round
        round, shuffled_next_round_csv_filepath = await default_pipeline(
            worker_settings,
            dataclass,
            results_filename,
            storage,
            logger,
        )

        # If the measurement is finished, clean the local files
        if round is None:
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
    measurement_id = f"{measurement_uuid}__{agent_settings.AGENT_UUID}"
    n_nodes = CountNodesFromResults().execute(
        agent_settings.database_url(), measurement_id
    )[0][0]
    n_links = CountLinks().execute(agent_settings.database_url(), measurement_id)[0][0]

    return {
        "measurement_uuid": measurement_uuid,
        "agent_uuid": agent_settings.AGENT_UUID,
        "database_name": agent_settings.DATABASE_NAME,
        "table_name": results_table(measurement_id),
        "n_rounds": n_rounds,
        "min_ttl": agent_settings.AGENT_MIN_TTL,
        "start_time": start_time,
        "end_time": datetime.now(),
        "n_nodes": n_nodes,
        "n_links": n_links,
    }
