import logging
import socket
import uuid
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
from diamond_miner.queries import Count, GetLinks, GetNodes, results_table
from fakeredis.aioredis import FakeRedis

from iris import __version__
from iris.agent.measurements import measurement
from iris.agent.settings import AgentSettings
from iris.agent.ttl import find_exit_ttl
from iris.commons.database import Agents, Database, Measurements
from iris.commons.redis import AgentRedis
from iris.commons.round import Round
from iris.commons.schemas.private import MeasurementRequest, MeasurementRoundRequest
from iris.commons.schemas.public import AgentParameters, AgentState
from iris.commons.schemas.public.measurements import (
    MeasurementAgentPostBody,
    MeasurementState,
    Tool,
    ToolParameters,
)
from iris.commons.settings import CommonSettings
from iris.commons.utils import get_ipv4_address, get_ipv6_address
from iris.standalone.storage import LocalStorage
from iris.worker.pipeline import default_pipeline
from iris.worker.settings import WorkerSettings


async def register_measurement(
    measurement_request: MeasurementRequest,
    settings: CommonSettings,
    logger: logging.Logger,
) -> None:
    database_measurements = Measurements(settings, logger)
    await database_measurements.create_table()
    await database_measurements.register(measurement_request)


async def register_agent(
    measurement_request: MeasurementRequest,
    agent_uuid: uuid.UUID,
    agent_parameters: AgentParameters,
    settings: CommonSettings,
    logger: logging.Logger,
) -> None:
    database_agents = Agents(settings, logger)
    await database_agents.create_table()
    await database_agents.register(measurement_request, agent_uuid, agent_parameters)


async def stamp_measurement(
    user: str, uuid: uuid.UUID, settings: CommonSettings, logger: logging.Logger
) -> None:
    database_measurements = Measurements(settings, logger)
    await database_measurements.stamp_finished(user, uuid)
    await database_measurements.stamp_end_time(user, uuid)


async def stamp_agent(
    measurement_uuid: uuid.UUID,
    agent_uuid: uuid.UUID,
    settings: CommonSettings,
    logger: logging.Logger,
) -> None:
    database_agents = Agents(settings, logger)
    await database_agents.stamp_finished(measurement_uuid, agent_uuid)


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

    # Enforce tool parameters
    tool_parameters.n_flow_ids = 6 if tool == Tool.DiamondMiner else 1
    tool_parameters.global_min_ttl = 1
    tool_parameters.global_max_ttl = 32

    # Get all settings
    agent_settings = AgentSettings()
    if logger.level >= logging.INFO:
        agent_settings.AGENT_CARACAL_LOGGING_LEVEL = logging.WARNING
    worker_settings = WorkerSettings()

    measurement_uuid = uuid.uuid4()
    start_time = datetime.now()

    # Find min TTL automatically
    if agent_settings.AGENT_MIN_TTL < 0:
        agent_settings.AGENT_MIN_TTL = find_exit_ttl(
            logger, agent_settings.AGENT_MIN_TTL_FIND_TARGET, min_ttl=2
        )

    # Instantiate Redis interface and register agent and measurement
    redis = AgentRedis(
        FakeRedis(decode_responses=True),
        agent_settings,
        logger,
        agent_settings.AGENT_UUID,
    )
    agent_parameters = AgentParameters(
        version=__version__,
        hostname=socket.gethostname(),
        ipv4_address=get_ipv4_address(),
        ipv6_address=get_ipv6_address(),
        min_ttl=agent_settings.AGENT_MIN_TTL,
        max_probing_rate=agent_settings.AGENT_MAX_PROBING_RATE,
        agent_tags=agent_settings.AGENT_TAGS,
    )
    await redis.register(365 * 24 * 60 * 60)
    await redis.set_agent_state(AgentState.Working)
    await redis.set_agent_parameters(agent_parameters)
    await redis.set_measurement_state(measurement_uuid, MeasurementState.Ongoing)

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

    statistics = {}

    round_ = Round(1, worker_settings.WORKER_ROUND_1_SLIDING_WINDOW, 0)
    n_rounds = 0
    while round_.number <= tool_parameters.max_round:
        n_rounds = round_.number
        request = MeasurementRequest(
            uuid=measurement_uuid,
            start_time=start_time,
            tool=tool,
            tags=tags,
            username=username,
            agents=[
                MeasurementAgentPostBody(
                    uuid=agent_settings.AGENT_UUID,
                    target_file=target_file.name,
                    probing_rate=probing_rate,
                    tool_parameters=tool_parameters,
                )
            ],
        )

        # If it's the first round,
        # register the measurement and the agent to the database
        if round_.number == 1:
            await register_measurement(request, worker_settings, logger)
            await register_agent(
                request,
                agent_settings.AGENT_UUID,
                agent_parameters,
                worker_settings,
                logger,
            )

        # Perform the measurement
        results_filename, round_statistics = await measurement(
            agent_settings,
            MeasurementRoundRequest(
                measurement=request,
                probes=shuffled_next_round_csv_filepath,
                round=round_,
            ),
            logger,
            redis,
            storage,
        )

        # Store the probing statistics of the round
        statistics[round_.encode()] = round_statistics

        # Compute the next round
        round_, shuffled_next_round_csv_filepath = await default_pipeline(
            worker_settings,
            request,
            agent_settings.AGENT_UUID,
            results_filename,
            round_statistics,
            logger,
            redis,
            storage,
        )

        # If the measurement is finished, clean the local files
        if round_ is None:
            if not worker_settings.WORKER_DEBUG_MODE:
                logger.info("Removing local measurement directory")
                try:
                    await aiofiles.os.rmdir(
                        worker_settings.WORKER_RESULTS_DIR_PATH / str(measurement_uuid)
                    )
                except OSError:
                    logger.error("Impossible to remove local measurement directory")
            break

    # Stamp the agent
    await stamp_agent(
        measurement_uuid, agent_settings.AGENT_UUID, agent_settings, logger
    )

    # Stamp the measurement
    await stamp_measurement(username, measurement_uuid, worker_settings, logger)

    # Compute distinct nodes/links
    measurement_id = f"{measurement_uuid}__{agent_settings.AGENT_UUID}"

    n_nodes = Count(query=GetNodes()).execute(
        agent_settings.database_url(), measurement_id
    )[0][0]
    n_links = Count(query=GetLinks()).execute(
        agent_settings.database_url(), measurement_id
    )[0][0]

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
        "probing_statistics": statistics,
    }
