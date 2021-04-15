"""Tool independant worker hook."""

import asyncio
import traceback

import dramatiq
from aiofiles import os as aios

from iris.commons.database import (
    DatabaseAgents,
    DatabaseAgentsSpecific,
    DatabaseMeasurements,
    get_session,
)
from iris.commons.dataclasses import ParametersDataclass
from iris.commons.logger import create_logger
from iris.commons.redis import Redis
from iris.commons.storage import Storage
from iris.worker.pipeline import default_pipeline, extract_round_number
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
        await asyncio.sleep(settings.WORKER_SANITY_CHECK_REFRESH)
    if False in checks:
        await sanity_clean(storage, measurement_uuid, agent_uuid, logger)
        return False
    return True


async def watch(redis, storage, parameters, logger):
    """Watch for results from an agent."""
    logger_prefix = f"{parameters.measurement_uuid} :: {parameters.agent_uuid} ::"

    session = get_session(settings)
    database_agents_specific = DatabaseAgentsSpecific(session, settings, logger=logger)

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
                await database_agents_specific.stamp_finished(
                    parameters.measurement_uuid, parameters.agent_uuid
                )
                break
            # Check if the measurement has been canceled
            measurement_state = await redis.get_measurement_state(
                parameters.measurement_uuid
            )
            if measurement_state is None:
                logger.warning(f"{logger_prefix} Measurement canceled")
                await database_agents_specific.stamp_finished(
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
            # The results file is not present, watch again
            await asyncio.sleep(settings.WORKER_WATCH_REFRESH)
            continue

        shuffled_next_round_csv_filename = await default_pipeline(
            settings, parameters, results_filename, storage, logger
        )

        if shuffled_next_round_csv_filename is None:
            logger.info(f"{logger_prefix} Measurement done for this agent")
            await database_agents_specific.stamp_finished(
                parameters.measurement_uuid, parameters.agent_uuid
            )
            break
        else:
            logger.info(f"{logger_prefix} Publish next mesurement")
            round_number = extract_round_number(shuffled_next_round_csv_filename)
            await redis.publish(
                parameters.agent_uuid,
                {
                    "measurement_uuid": parameters.measurement_uuid,
                    "username": parameters.user,
                    "parameters": parameters.dict(),
                    "round": round_number,
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
    session = get_session(settings)
    database_measurements = DatabaseMeasurements(session, settings, logger=logger)
    database_agents = DatabaseAgents(session, settings, logger=logger)
    database_agents_specific = DatabaseAgentsSpecific(session, settings, logger=logger)

    redis = Redis(settings=settings, logger=logger)
    await redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    logger.info(f"{logger_prefix} Getting agents information")
    agents = []
    for agent_uuid, specific_parameters in agents_information.items():
        physical_parameters = await redis.get_agent_parameters(agent_uuid)
        if physical_parameters:
            agents.append(
                ParametersDataclass(
                    agent_uuid,
                    measurement_parameters,
                    physical_parameters,
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
        await database_agents_specific.create_table()
        for agent in agents:
            # Register information about the physical agent
            is_already_present = await database_agents.get(agent.agent_uuid)
            if is_already_present is None:
                # Physical agent not present, registering
                await database_agents.register(
                    agent.agent_uuid, agent.physical_parameters
                )
            else:
                # Already present, updating last used
                await database_agents.stamp_last_used(agent.agent_uuid)

            # Register agent in this measurement and specific information
            await database_agents_specific.register(agent)

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
            "round": 1,
            "probes": None,
            "parameters": measurement_parameters,
        }

        for agent in agents:
            request["parameters"] = agent.dict()
            await redis.publish(agent.agent_uuid, request)

    else:
        # We are in this state when the worker has failed and replays the measurement
        # So we stip off the agents those which are finished
        agent_specific_info = await database_agents_specific.all(measurement_uuid)
        finished_agents = [
            a["uuid"] for a in agent_specific_info if a["state"] == "finished"
        ]
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

    logger.info(f"{logger_prefix} Stamp the end time for measurement")
    await database_measurements.stamp_end_time(username, measurement_uuid)

    logger.info(f"{logger_prefix} Measurement done")
    await redis.delete_measurement_state(measurement_uuid)
    await redis.disconnect()


@dramatiq.actor(
    time_limit=settings.WORKER_TIME_LIMIT, max_age=settings.WORKER_MESSAGE_AGE_LIMIT
)
def hook(agents_specific, measurement_parameters):
    """Hook a worker process to a measurement"""
    logger = create_logger(settings)
    try:
        asyncio.run(callback(agents_specific, measurement_parameters, logger))
    except Exception as exception:
        measurement_uuid = measurement_parameters["measurement_uuid"]
        traceback_content = traceback.format_exc()
        for line in traceback_content.splitlines():
            logger.critical(f"{measurement_uuid} :: {line}")
        raise exception
