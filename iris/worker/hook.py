"""Tool independant worker hook."""

import asyncio
import random
import traceback
from logging import Logger
from uuid import UUID

import dramatiq
from aiofiles import os as aios

from iris.commons.database import Agents, Database, Measurements
from iris.commons.logger import create_logger
from iris.commons.redis import Redis
from iris.commons.round import Round
from iris.commons.schemas.private import MeasurementRequest, MeasurementRoundRequest
from iris.commons.schemas.public import MeasurementState
from iris.commons.storage import Storage
from iris.worker.pipeline import default_pipeline
from iris.worker.settings import WorkerSettings

settings = WorkerSettings()


async def sanity_clean(
    measurement_uuid: UUID, agent_uuid: UUID, logger: Logger, storage: Storage
):
    """Clean AWS S3 if the sanity check don't pass."""
    remote_files = await storage.get_all_files(str(measurement_uuid))
    for remote_file in remote_files:
        remote_filename = remote_file["key"]
        if remote_filename.startswith(str(agent_uuid)):
            logger.warning(f"Sanity remove `{remote_filename}` from AWS S3")
            is_deleted = await storage.delete_file_no_check(
                str(measurement_uuid), remote_filename
            )
            if not is_deleted:
                logger.error(f"Impossible to remove `{remote_filename}`")


async def sanity_check(
    measurement_uuid: UUID,
    agent_uuid: UUID,
    logger: Logger,
    redis: Redis,
    storage: Storage,
):
    """
    Sanity check to close the loop if the agent is disconnected.
    Returns: True if the agent is alive, else False.
    """
    checks = []
    for _ in range(settings.WORKER_SANITY_CHECK_RETRIES):
        checks.append(await redis.check_agent(agent_uuid))
        refresh_time = random.uniform(
            settings.WORKER_SANITY_CHECK_REFRESH_MIN,
            settings.WORKER_SANITY_CHECK_REFRESH_MAX,
        )
        await asyncio.sleep(refresh_time)
    if False in checks:
        await sanity_clean(measurement_uuid, agent_uuid, logger, storage)
        return False
    return True


async def watch(
    measurement_request: MeasurementRequest,
    agent_uuid: UUID,
    logger: Logger,
    redis: Redis,
    storage: Storage,
):
    """Watch for results from an agent."""
    agent = measurement_request.agent(agent_uuid)
    assert agent.uuid
    logger_prefix = f"{measurement_request.uuid} :: {agent.uuid} ::"
    database = Database(settings, logger)
    database_agents = Agents(database)

    while True:
        if settings.WORKER_SANITY_CHECK_ENABLE:
            # Check if the agent is down
            is_agent_alive = await sanity_check(
                measurement_request.uuid, agent.uuid, logger, redis, storage
            )
            if not is_agent_alive:
                logger.warning(f"{logger_prefix} Stop watching agent")
                await database_agents.stamp_finished(
                    measurement_request.uuid, agent.uuid
                )
                break
            # Check if the measurement has been canceled
            measurement_state = await redis.get_measurement_state(
                measurement_request.uuid
            )
            if measurement_state in [
                MeasurementState.Canceled,
                MeasurementState.Unknown,
            ]:
                logger.warning(f"{logger_prefix} Measurement canceled")
                await database_agents.stamp_canceled(
                    measurement_request.uuid, agent.uuid
                )
                break

        # Search for results file
        remote_files = await storage.get_all_files(str(measurement_request.uuid))
        for remote_file in remote_files:
            remote_filename = remote_file["key"]
            if remote_filename.startswith(f"{agent.uuid}_results"):
                results_filename = remote_filename
                break
        else:
            # The results file is not present, watch again (with random delay)
            refresh_time = random.uniform(
                settings.WORKER_SANITY_CHECK_REFRESH_MIN,
                settings.WORKER_SANITY_CHECK_REFRESH_MAX,
            )
            await asyncio.sleep(refresh_time)
            continue

        # Get the statistics from Redis
        statistics = await redis.get_measurement_stats(
            measurement_request.uuid, agent.uuid
        )

        next_round, shuffled_next_round_csv_filename = await default_pipeline(
            settings,
            measurement_request,
            agent.uuid,
            results_filename,
            statistics,
            logger,
            redis,
            storage,
        )

        # Remove the statistics from Redis
        await redis.delete_measurement_stats(measurement_request.uuid, agent.uuid)

        if next_round is None:
            logger.info(f"{logger_prefix} Measurement done for this agent")
            await database_agents.stamp_finished(measurement_request.uuid, agent.uuid)
            break
        else:
            logger.info(f"{logger_prefix} Publish next measurement")
            request = MeasurementRoundRequest(
                measurement=measurement_request,
                probes=shuffled_next_round_csv_filename,
                round=next_round,
            )
            await redis.publish(agent.uuid, request)


async def callback(measurement_request: MeasurementRequest, logger: Logger):
    """Asynchronous callback."""
    logger_prefix = f"{measurement_request.uuid} ::"
    logger.info(f"{logger_prefix} New measurement received")

    database = Database(settings, logger)
    database_agents = Agents(database)
    database_measurements = Measurements(database)
    redis = Redis(await settings.redis_client(), settings, logger)
    storage = Storage(settings, logger)

    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / str(
        measurement_request.uuid
    )

    if (
        await redis.get_measurement_state(measurement_request.uuid)
        is MeasurementState.Unknown
    ):
        # There is no measurement state, so the measurement hasn't started yet
        logger.info(f"{logger_prefix} Set measurement state to `waiting`")
        await redis.set_measurement_state(
            measurement_request.uuid, MeasurementState.Waiting
        )

        logger.info(f"{logger_prefix} Create local measurement directory")
        try:
            await aios.mkdir(str(measurement_results_path))
        except FileExistsError:
            logger.warning(f"{logger_prefix} Local measurement directory already exits")

        logger.info(f"{logger_prefix} Register measurement into database")
        await database_measurements.create_table()
        await database_measurements.register(measurement_request)

        logger.info(f"{logger_prefix} Register agents into database")
        await database_agents.create_table()
        for agent in measurement_request.agents:
            # Register agent in this measurement
            assert agent.uuid
            agent_parameters = await redis.get_agent_parameters(agent.uuid)
            if agent_parameters:
                await database_agents.register(
                    measurement_request, agent.uuid, agent_parameters
                )

        logger.info(f"{logger_prefix} Archive target files")
        for agent in measurement_request.agents:
            # noinspection PyBroadException
            try:
                await storage.copy_file_to_bucket(
                    settings.AWS_S3_TARGETS_BUCKET_PREFIX
                    + measurement_request.username,
                    settings.AWS_S3_ARCHIVE_BUCKET_PREFIX
                    + measurement_request.username,
                    agent.target_file,
                    f"targets__{measurement_request.uuid}__{agent.uuid}.csv",
                )
            except Exception:
                logger.error(f"{logger_prefix} Impossible to archive target files")
                return

        logger.info(f"{logger_prefix} Create bucket in AWS S3")
        # noinspection PyBroadException
        try:
            await storage.create_bucket(bucket=str(measurement_request.uuid))
        except Exception:
            logger.error(f"{logger_prefix} Impossible to create bucket")
            return

        logger.info(f"{logger_prefix} Publish measurement to agents")
        request = MeasurementRoundRequest(
            measurement=measurement_request,
            probes=None,
            round=Round(1, settings.WORKER_ROUND_1_SLIDING_WINDOW, 0),
        )

        for agent in measurement_request.agents:
            await redis.publish(agent.uuid, request)
        agents = measurement_request.agents

    else:
        # We are in this state when the worker has failed and replays the measurement
        # So we skip off the agents those which are finished
        agent_info = await database_agents.all(measurement_request.uuid)
        finished_agents = [a["uuid"] for a in agent_info if a["state"] == "finished"]
        filtered_agents = []
        for agent in measurement_request.agents:
            if agent.uuid not in finished_agents:
                filtered_agents.append(agent)
        agents = filtered_agents

    logger.info(f"{logger_prefix} Watching ...")
    await asyncio.gather(
        *[
            watch(measurement_request, agent.uuid, logger, redis, storage)  # type: ignore
            for agent in agents
        ]
    )

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Removing local measurement directory")
        try:
            await aios.rmdir(measurement_results_path)
        except OSError:
            logger.error(
                f"{logger_prefix} Impossible to remove local measurement directory"
            )

    logger.info(f"{logger_prefix} Delete bucket")
    # noinspection PyBroadException
    try:
        await storage.delete_all_files_from_bucket(bucket=str(measurement_request.uuid))
        await storage.delete_bucket(bucket=str(measurement_request.uuid))
    except Exception:
        logger.error(f"{logger_prefix} Impossible to remove bucket")

    logger.info(f"{logger_prefix} Stamp measurement state")
    if (
        await redis.get_measurement_state(measurement_request.uuid)
        == MeasurementState.Canceled
    ):
        await database_measurements.stamp_canceled(
            measurement_request.username, measurement_request.uuid
        )
    else:
        await database_measurements.stamp_finished(
            measurement_request.username, measurement_request.uuid
        )

    logger.info(f"{logger_prefix} Stamp measurement end time")
    await database_measurements.stamp_end_time(
        measurement_request.username, measurement_request.uuid
    )

    logger.info(f"{logger_prefix} Measurement done")
    await redis.delete_measurement_state(measurement_request.uuid)
    await redis.disconnect()


# noinspection PyTypeChecker
@dramatiq.actor(
    time_limit=settings.WORKER_TIME_LIMIT, max_age=settings.WORKER_MESSAGE_AGE_LIMIT
)
def hook(measurement_request: MeasurementRequest):
    """Hook a worker process to a measurement"""
    logger = create_logger(settings)
    try:
        asyncio.run(callback(measurement_request, logger))
    except Exception as exception:
        traceback_content = traceback.format_exc()
        for line in traceback_content.splitlines():
            logger.critical(f"{measurement_request.uuid} :: {line}")
        raise exception
