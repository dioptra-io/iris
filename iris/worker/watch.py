import asyncio
from datetime import datetime
from typing import Optional

import dramatiq
from sqlmodel import Session

from iris.commons.clickhouse import ClickHouse
from iris.commons.logger import Adapter, base_logger
from iris.commons.models import (
    MeasurementAgent,
    MeasurementAgentState,
    MeasurementRoundRequest,
)
from iris.commons.redis import Redis
from iris.commons.storage import Storage
from iris.worker.outer_pipeline import outer_pipeline
from iris.worker.settings import WorkerSettings

default_settings = WorkerSettings()


@dramatiq.actor(
    time_limit=default_settings.WORKER_TIME_LIMIT,
    max_age=default_settings.WORKER_MESSAGE_AGE_LIMIT,
)
def watch_measurement_agent(measurement_uuid: str, agent_uuid: str):
    asyncio.run(
        watch_measurement_agent_(measurement_uuid, agent_uuid, default_settings)
    )


async def watch_measurement_agent_(
    measurement_uuid: str, agent_uuid: str, settings: WorkerSettings
):
    logger = Adapter(
        base_logger,
        dict(
            component="worker", measurement_uuid=measurement_uuid, agent_uuid=agent_uuid
        ),
    )

    clickhouse = ClickHouse(settings, logger)
    redis = Redis(await settings.redis_client(), settings, logger)
    session = Session(settings.sqlalchemy_engine())
    storage = Storage(settings, logger)

    ma = MeasurementAgent.get(session, measurement_uuid, agent_uuid)
    if not ma:
        logger.error("Measurement not found")
        return
    logger.info("Watching measurement agent in state %s", ma.state)

    while True:
        # 1. Ensure that the MeasurementAgent instance is up-to-date.
        session.refresh(ma)

        # 2. Ensure that the measurement is not already done.
        if ma.state not in {
            MeasurementAgentState.Created,
            MeasurementAgentState.Ongoing,
        }:
            break

        # 3. Ensure that the agent is still alive.
        agent_ok = await check_agent(
            redis,
            agent_uuid,
            settings.WORKER_SANITY_CHECK_RETRIES,
            settings.WORKER_SANITY_CHECK_INTERVAL,
        )
        if not agent_ok:
            ma.set_state(session, MeasurementAgentState.AgentFailure)
            break

        # 4. Find the results file.
        results_filename = None
        # 4.a. If the measurement was just created, do not wait for results.
        if ma.state == MeasurementAgentState.Created:
            ma.set_state(session, MeasurementAgentState.Ongoing)
            ma.set_start_time(session, datetime.utcnow())
        # 4.b. Otherwise, check if a results file is available on S3.
        elif ma.state == MeasurementAgentState.Ongoing:
            results_filename = await find_results(storage, measurement_uuid, agent_uuid)
            if not results_filename:
                # 4.b.1. If the results file is not present, try again later.
                await asyncio.sleep(settings.WORKER_WATCH_INTERVAL)
                continue

        if probing_statistics := await redis.get_measurement_stats(
            measurement_uuid, agent_uuid
        ):
            ma.append_probing_statistics(session, probing_statistics)
            await redis.delete_measurement_stats(measurement_uuid, agent_uuid)

        # TODO: Create a null tool that does nothing that would allow to test the full pipeline.
        # This tool would generate 3 dummy rounds.
        result = await outer_pipeline(
            clickhouse=clickhouse,
            storage=storage,
            redis=redis,
            logger=logger,
            measurement_uuid=measurement_uuid,
            agent_uuid=agent_uuid,
            measurement_tags=ma.measurement.tags,
            sliding_window_size=settings.WORKER_ROUND_1_SLIDING_WINDOW,
            sliding_window_stopping_condition=settings.WORKER_ROUND_1_STOPPING,
            tool=ma.measurement.tool,
            tool_parameters=ma.tool_parameters,
            working_directory=settings.WORKER_RESULTS_DIR_PATH / measurement_uuid,
            targets_key=ma.target_file,
            results_key=results_filename,
            user_id=ma.measurement.user_id,
            max_open_files=settings.WORKER_MAX_OPEN_FILES,
            debug_mode=settings.WORKER_DEBUG_MODE,
        )

        if not result:
            ma.set_state(session, MeasurementAgentState.Finished)
            break

        await redis.publish(
            agent_uuid,
            MeasurementRoundRequest(
                measurement=ma.measurement,
                measurement_agent=ma,
                probe_filename=result.probes_key,
                round=result.next_round,
            ),
        )

    logger.info("Done watching measurement agent in state %s, cleaning...", ma.state)

    if not ma.end_time:
        ma.set_end_time(session, datetime.utcnow())

    await clean_results(
        measurement_uuid=measurement_uuid,
        agent_uuid=agent_uuid,
        storage=storage,
    )


async def check_agent(
    redis: Redis, agent_uuid: str, trials: int, interval: float
) -> bool:
    checks = []
    for _ in range(trials):
        checks.append(await redis.check_agent(agent_uuid))
        await asyncio.sleep(interval)
    return any(checks)


async def clean_results(
    storage: Storage, measurement_uuid: str, agent_uuid: str
) -> None:
    bucket = storage.measurement_agent_bucket(measurement_uuid, agent_uuid)
    await storage.delete_all_files_from_bucket(bucket)
    await storage.delete_bucket(bucket)


async def find_results(
    storage: Storage, measurement_uuid: str, agent_uuid: str
) -> Optional[str]:
    bucket = storage.measurement_agent_bucket(measurement_uuid, agent_uuid)
    files = await storage.get_all_files(bucket)
    for file in files:
        if file["key"].startswith("results_"):
            return str(file["key"])
    return None
