"""Measurement interface."""
import shutil
from datetime import datetime
from logging import Logger
from multiprocessing import Manager, Process

from iris.agent.prober import probe, watcher
from iris.agent.settings import AgentSettings
from iris.commons.redis import AgentRedis
from iris.commons.schemas.measurements import MeasurementRoundRequest, ProbingStatistics
from iris.commons.storage import Storage, results_key


async def measurement(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    logger: Logger,
    redis: AgentRedis,
    storage: Storage,
):
    """Conduct a measurement."""
    measurement_request = request.measurement
    agent = measurement_request.agent(settings.AGENT_UUID)
    assert agent.uuid  # make mypy happy
    logger_prefix = f"{measurement_request.uuid} :: {agent.uuid} ::"

    logger.info(f"{logger_prefix} Create local measurement directory")
    measurement_results_path = settings.AGENT_RESULTS_DIR_PATH / str(
        measurement_request.uuid
    )
    measurement_results_path.mkdir(exist_ok=True)

    probes_filepath = None
    results_filepath = measurement_results_path / results_key(agent.uuid, request.round)

    logger.info(f"{logger_prefix} Download CSV probe file locally")
    probes_filepath = await storage.download_file_to(
        storage.measurement_bucket(measurement_request.uuid),
        request.probe_filename,
        settings.AGENT_TARGETS_DIR_PATH,
    )

    logger.info(f"{logger_prefix} User ID : {measurement_request.user_id}")
    logger.info(f"{logger_prefix} Probe File: {request.probe_filename}")
    logger.info(f"{logger_prefix} {request.round}")
    logger.info(f"{logger_prefix} Tool : {measurement_request.tool}")
    logger.info(f"{logger_prefix} Tool Parameters : {agent.tool_parameters}")
    logger.info(f"{logger_prefix} Max Probing Rate : {agent.probing_rate}")

    probing_start_time = datetime.utcnow()
    with Manager() as manager:
        prober_statistics = manager.dict()  # type: ignore

        prober_process = Process(
            target=probe,
            args=(
                settings,
                results_filepath,
                request.round.number,
                agent.probing_rate,
                prober_statistics,
                probes_filepath,
            ),
        )

        prober_process.start()
        is_not_canceled = await watcher(
            prober_process, settings, measurement_request.uuid, redis
        )
        prober_statistics = dict(prober_statistics)

    if is_not_canceled:
        logger.info("Upload probing statistics in Redis")
        statistics = ProbingStatistics(
            round=request.round,
            start_time=probing_start_time,
            end_time=datetime.utcnow(),
            **prober_statistics,
        )
        await redis.set_measurement_stats(
            measurement_request.uuid, agent.uuid, statistics
        )

        logger.info(f"{logger_prefix} Upload results file into S3")
        await storage.upload_file(
            storage.measurement_bucket(measurement_request.uuid),
            results_key(agent.uuid, request.round),
            results_filepath,
        )
    else:
        logger.warning(f"{logger_prefix} Measurement canceled")

    if not settings.AGENT_DEBUG_MODE:
        logger.info(f"{logger_prefix} Empty local results directory")
        shutil.rmtree(settings.AGENT_RESULTS_DIR_PATH)
        settings.AGENT_RESULTS_DIR_PATH.mkdir()

        logger.info(f"{logger_prefix} Empty local targets directory")
        shutil.rmtree(settings.AGENT_TARGETS_DIR_PATH)
        settings.AGENT_TARGETS_DIR_PATH.mkdir()

        if request.probe_filename:
            logger.info(f"{logger_prefix} Remove prefix file from S3")
            await storage.soft_delete(
                storage.measurement_bucket(measurement_request.uuid),
                request.probe_filename,
            )
