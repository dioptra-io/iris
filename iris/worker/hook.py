"""Tool independant worker hook."""

import asyncio
import random
import traceback

import dramatiq
from aiofiles import os as aios

from iris.commons.database import Agents, Measurements
from iris.commons.dataclasses import ParametersDataclass
from iris.commons.logger import create_logger
from iris.commons.redis import Redis
from iris.commons.round import Round
from iris.commons.storage import Storage
from iris.worker.pipeline import default_pipeline
from iris.worker.settings import WorkerSettings

settings = WorkerSettings()


async def sanity_clean(storage, measurement_uuid, agent_uuid, logger):
    """Clean AWS S3 if the sanity check don't pass."""
    remote_files = await storage.get_all_files(measurement_uuid)
    for remote_file in remote_files:
        remote_filename = remote_file["key"]
        if remote_filename.startswith(agent_uuid):
            logger.warning(f"Sanity remove `{remote_filename}` from AWS S3")
            is_deleted = await storage.delete_file_no_check(
                measurement_uuid, remote_filename
            )
            if not is_deleted:
                logger.error(f"Impossible to remove `{remote_filename}`")


async def sanity_check(redis, storage, measurement_uuid, agent_uuid, logger):
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
        await sanity_clean(storage, measurement_uuid, agent_uuid, logger)
        return False
    return True


async def watch(redis, storage, parameters, logger):
    """Watch for results from an agent."""
    logger_prefix = f"{parameters.measurement_uuid} :: {parameters.agent_uuid} ::"
    database_agents = Agents(settings, logger)

    while True:
        if settings.WORKER_SANITY_CHECK_ENABLE:
            # Check if the agent is down
            is_agent_alive = await sanity_check(
                redis,
                storage,
                parameters.measurement_uuid,
                parameters.agent_uuid,
                logger,
            )
            if not is_agent_alive:
                logger.warning(f"{logger_prefix} Stop watching agent")
                await database_agents.stamp_finished(
                    parameters.measurement_uuid, parameters.agent_uuid
                )
                break
            # Check if the measurement has been canceled
            measurement_state = await redis.get_measurement_state(
                parameters.measurement_uuid
            )
            if measurement_state is None or measurement_state == "canceled":
                logger.warning(f"{logger_prefix} Measurement canceled")
                await database_agents.stamp_canceled(
                    parameters.measurement_uuid, parameters.agent_uuid
                )
                break

        # Search for results file
        results_filename = None
        remote_files = await storage.get_all_files(parameters.measurement_uuid)
        for remote_file in remote_files:
            remote_filename = remote_file["key"]
            if remote_filename.startswith(f"{parameters.agent_uuid}_results"):
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
            parameters.measurement_uuid, parameters.agent_uuid
        )

        next_round, shuffled_next_round_csv_filename = await default_pipeline(
            settings, parameters, results_filename, statistics, storage, logger
        )

        # Remove the statistics from Redis
        await redis.delete_measurement_stats(
            parameters.measurement_uuid, parameters.agent_uuid
        )

        if next_round is None:
            logger.info(f"{logger_prefix} Measurement done for this agent")
            await database_agents.stamp_finished(
                parameters.measurement_uuid, parameters.agent_uuid
            )
            break
        else:
            logger.info(f"{logger_prefix} Publish next mesurement")
            await redis.publish(
                parameters.agent_uuid,
                {
                    "measurement_uuid": parameters.measurement_uuid,
                    "username": parameters.user,
                    "parameters": parameters.dict(),
                    "round": next_round.encode(),
                    "probes": shuffled_next_round_csv_filename,
                },
            )


async def callback(agents_information, measurement_parameters, logger):
    """Asynchronous callback."""
    measurement_uuid = measurement_parameters["measurement_uuid"]
    username = measurement_parameters["user"]

    storage = Storage(settings, logger)

    logger_prefix = f"{measurement_uuid} ::"

    logger.info(f"{logger_prefix} New measurement received")

    database_measurements = Measurements(settings, logger)
    database_agents = Agents(settings, logger)
    redis = Redis(settings, logger)

    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    logger.info(f"{logger_prefix} Getting agents information")
    agents = []
    for agent_uuid, specific_parameters in agents_information.items():
        agent_parameters = await redis.get_agent_parameters(agent_uuid)
        if agent_parameters:
            agents.append(
                ParametersDataclass(
                    agent_uuid,
                    measurement_parameters,
                    agent_parameters,
                    specific_parameters,
                )
            )

    if not agents:
        logger.error(f"{logger_prefix} Measurement stopped because no agent found")

    measurement_results_path = settings.WORKER_RESULTS_DIR_PATH / measurement_uuid

    if await redis.get_measurement_state(measurement_uuid) is None:
        # There is no measurement state, so the measurement hasn't started yet
        logger.info(f"{logger_prefix} Set measurement state to `waiting`")
        await redis.set_measurement_state(measurement_uuid, "waiting")

        logger.info(f"{logger_prefix} Create local measurement directory")
        try:
            await aios.mkdir(str(measurement_results_path))
        except FileExistsError:
            logger.warning(f"{logger_prefix} Local measurement directory already exits")

        logger.info(f"{logger_prefix} Register measurement into database")
        await database_measurements.create_table()
        await database_measurements.register(measurement_parameters)

        logger.info(f"{logger_prefix} Register agents into database")
        await database_agents.create_table()
        for agent in agents:
            # Register agent in this measurement
            await database_agents.register(agent)

        logger.info(f"{logger_prefix} Archive target files")
        for agent in agents:
            try:
                await storage.copy_file_to_bucket(
                    settings.AWS_S3_TARGETS_BUCKET_PREFIX + agent.user,
                    settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + agent.user,
                    agent.target_file,
                    f"targets__{agent.measurement_uuid}__{agent.agent_uuid}.csv",
                )
            except Exception:
                logger.error(f"{logger_prefix} Impossible to archive target files")
                return

        logger.info(f"{logger_prefix} Create bucket in AWS S3")
        try:
            await storage.create_bucket(bucket=measurement_uuid)
        except Exception:
            logger.error(f"{logger_prefix} Impossible to create bucket")
            return

        logger.info(f"{logger_prefix} Publish measurement to agents")
        request = {
            "measurement_uuid": measurement_uuid,
            "username": username,
            "round": Round(1, settings.WORKER_ROUND_1_SLIDING_WINDOW, 0).encode(),
            "probes": None,
        }

        for agent in agents:
            request["parameters"] = agent.dict()
            await redis.publish(agent.agent_uuid, request)

    else:
        # We are in this state when the worker has failed and replays the measurement
        # So we stip off the agents those which are finished
        agent_info = await database_agents.all(measurement_uuid)
        finished_agents = [a["uuid"] for a in agent_info if a["state"] == "finished"]
        filtered_agents = []
        for agent in agents:
            if agent.agent_uuid not in finished_agents:
                filtered_agents.append(agent)
        agents = filtered_agents

    logger.info(f"{logger_prefix} Watching ...")
    await asyncio.gather(*[watch(redis, storage, agent, logger) for agent in agents])

    if not settings.WORKER_DEBUG_MODE:
        logger.info(f"{logger_prefix} Removing local measurement directory")
        try:
            await aios.rmdir(measurement_results_path)
        except OSError:
            logger.error(
                f"{logger_prefix} Impossible to remove local measurement directory"
            )

    logger.info(f"{logger_prefix} Delete bucket")
    try:
        await storage.delete_all_files_from_bucket(bucket=measurement_uuid)
        await storage.delete_bucket(bucket=measurement_uuid)
    except Exception:
        logger.error(f"{logger_prefix} Impossible to remove bucket")

    logger.info(f"{logger_prefix} Stamp measurement state")
    if await redis.get_measurement_state(measurement_uuid) == "canceled":
        await database_measurements.stamp_canceled(username, measurement_uuid)
    else:
        await database_measurements.stamp_finished(username, measurement_uuid)

    logger.info(f"{logger_prefix} Stamp measurement end time")
    await database_measurements.stamp_end_time(username, measurement_uuid)

    logger.info(f"{logger_prefix} Measurement done")
    await redis.delete_measurement_state(measurement_uuid)
    await redis.disconnect()


@dramatiq.actor(
    time_limit=settings.WORKER_TIME_LIMIT, max_age=settings.WORKER_MESSAGE_AGE_LIMIT
)
def hook(agents, measurement_parameters):
    """Hook a worker process to a measurement"""
    logger = create_logger(settings)
    try:
        asyncio.run(callback(agents, measurement_parameters, logger))
    except Exception as exception:
        measurement_uuid = measurement_parameters["measurement_uuid"]
        traceback_content = traceback.format_exc()
        for line in traceback_content.splitlines():
            logger.critical(f"{measurement_uuid} :: {line}")
        raise exception
