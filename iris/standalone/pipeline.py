import logging
import socket
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from uuid import UUID, uuid4

import aiofiles.os
from diamond_miner.queries import Count, GetLinks, GetNodes, results_table
from fakeredis.aioredis import FakeRedis

from iris import __version__
from iris.agent.measurements import measurement
from iris.agent.settings import AgentSettings
from iris.agent.ttl import find_exit_ttl
from iris.commons.database import Database, InsertResults, agents, measurements
from iris.commons.redis import AgentRedis
from iris.commons.schemas.private import MeasurementRequest, MeasurementRoundRequest
from iris.commons.schemas.public import AgentParameters, AgentState
from iris.commons.schemas.public.measurements import (
    MeasurementAgentPostBody,
    MeasurementState,
    Tool,
    ToolParameters,
)
from iris.commons.storage import results_key
from iris.commons.utils import get_ipv4_address, get_ipv6_address
from iris.standalone.storage import LocalStorage
from iris.worker.outer_pipeline import outer_pipeline
from iris.worker.settings import WorkerSettings


async def register_measurement(
    database: Database, measurement_request: MeasurementRequest
) -> None:
    await measurements.create_table(database)
    await measurements.register(database, measurement_request)


async def register_agent(
    database: Database,
    measurement_request: MeasurementRequest,
    agent_uuid: UUID,
    agent_parameters: AgentParameters,
) -> None:
    await agents.create_table(database)
    await agents.register(database, measurement_request, agent_uuid, agent_parameters)


async def stamp_measurement(database: Database, user: str, uuid: UUID) -> None:
    await measurements.stamp_finished(database, user, uuid)
    await measurements.stamp_end_time(database, user, uuid)


async def stamp_agent(
    database: Database, measurement_uuid: UUID, agent_uuid: UUID
) -> None:
    await agents.stamp_finished(database, measurement_uuid, agent_uuid)


async def pipeline(
    tool: Tool,
    targets: list,
    username: str,
    probing_rate: int,
    tool_parameters: ToolParameters,
    tags: List[str],
    s3_dir: Path,
    logger,
) -> Dict:
    """Measurement pipeline."""
    # Silence the tzlocal timezone configuration warning
    warnings.filterwarnings("ignore", category=UserWarning, module="tzlocal")

    # Enforce tool parameters
    tool_parameters = tool_parameters.copy(
        update={"global_min_ttl": 1, "global_max_ttl": 32}
    )

    # Get all settings
    agent_settings = AgentSettings()
    if logger.level >= logging.INFO:
        agent_settings.AGENT_CARACAL_LOGGING_LEVEL = logging.WARNING
    worker_settings = WorkerSettings()

    measurement_uuid = uuid4()
    start_time = datetime.utcnow()

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
    database = Database(agent_settings, logger)
    await database.create_database()

    # Create a target file
    target_file: Path = (
        agent_settings.AGENT_TARGETS_DIR_PATH
        / f"targets__{measurement_uuid}__{agent_settings.AGENT_UUID}.csv"
    )
    async with aiofiles.open(target_file, mode="w") as fd:
        for target in targets:
            await fd.write(target)

    # Copy the target file to the local storage
    storage = LocalStorage(agent_settings, s3_dir)
    await storage.upload_file(
        storage.targets_bucket(username),
        target_file.name,
        target_file,
    )
    await storage.upload_file(
        storage.archive_bucket(username),
        target_file.name,
        target_file,
    )

    statistics = {}

    # TODO: Do we really need all of this just to create the probes table?
    insert_results = InsertResults(
        database,
        measurement_uuid,
        agent_settings.AGENT_UUID,
        tool_parameters.prefix_len_v4,
        tool_parameters.prefix_len_v6,
    )
    logger.info("Create measurement tables")
    await insert_results.create_table()
    # ----

    # Round 1.0
    pipeline_result = await outer_pipeline(
        database=database,
        storage=storage,
        redis=redis,
        logger=logger,
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_settings.AGENT_UUID,
        sliding_window_size=worker_settings.WORKER_ROUND_1_SLIDING_WINDOW,
        sliding_window_stopping_condition=worker_settings.WORKER_ROUND_1_STOPPING,
        tool=tool,
        tool_parameters=tool_parameters,
        working_directory=(
            worker_settings.WORKER_RESULTS_DIR_PATH / str(measurement_uuid)
        ),
        targets_key=target_file.name,
        results_key=None,
        username=username,
        debug_mode=worker_settings.WORKER_DEBUG_MODE,
    )
    assert pipeline_result

    round_ = pipeline_result.next_round
    probe_filename = pipeline_result.probes_key

    while round_.number <= tool_parameters.max_round:
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
            await register_measurement(database, request)
            await register_agent(
                database, request, agent_settings.AGENT_UUID, agent_parameters
            )

        # Perform the measurement
        await measurement(
            agent_settings,
            MeasurementRoundRequest(
                measurement=request,
                probe_filename=probe_filename,
                round=round_,
            ),
            logger,
            redis,
            storage,
        )

        # Store the probing statistics of the round
        round_statistics = await redis.get_measurement_stats(
            measurement_uuid, agent_settings.AGENT_UUID
        )
        statistics[round_.encode()] = round_statistics

        # Compute the next round
        pipeline_result = await outer_pipeline(
            database=database,
            storage=storage,
            redis=redis,
            logger=logger,
            measurement_uuid=measurement_uuid,
            agent_uuid=agent_settings.AGENT_UUID,
            sliding_window_size=worker_settings.WORKER_ROUND_1_SLIDING_WINDOW,
            sliding_window_stopping_condition=worker_settings.WORKER_ROUND_1_STOPPING,
            tool=tool,
            tool_parameters=tool_parameters,
            working_directory=(
                worker_settings.WORKER_RESULTS_DIR_PATH / str(measurement_uuid)
            ),
            targets_key=target_file.name,
            results_key=results_key(agent_settings.AGENT_UUID, round_),
            username=username,
            debug_mode=worker_settings.WORKER_DEBUG_MODE,
        )

        # If the measurement is finished, clean the local files
        if not pipeline_result:
            if not worker_settings.WORKER_DEBUG_MODE:
                logger.info("Removing local measurement directory")
                try:
                    await aiofiles.os.rmdir(
                        worker_settings.WORKER_RESULTS_DIR_PATH / str(measurement_uuid)
                    )
                except OSError:
                    logger.error("Impossible to remove local measurement directory")
            break

        round_ = pipeline_result.next_round
        probe_filename = pipeline_result.probes_key

    # Stamp the agent
    await stamp_agent(database, measurement_uuid, agent_settings.AGENT_UUID)

    # Stamp the measurement
    await stamp_measurement(database, username, measurement_uuid)

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
        "n_rounds": round_.number,
        "min_ttl": agent_settings.AGENT_MIN_TTL,
        "start_time": start_time,
        "end_time": datetime.utcnow(),
        "n_nodes": n_nodes,
        "n_links": n_links,
        "probing_statistics": statistics,
    }
