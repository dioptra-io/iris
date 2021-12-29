"""Measurement interface."""
import shutil
from datetime import datetime
from logging import Logger, LoggerAdapter
from multiprocessing import Manager, Process

from iris.agent.prober import probe, watcher
from iris.agent.settings import AgentSettings
from iris.commons.models.diamond_miner import ProbingStatistics
from iris.commons.models.measurement_round_request import MeasurementRoundRequest
from iris.commons.redis import AgentRedis
from iris.commons.storage import Storage, results_key


async def do_measurement(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    logger: LoggerAdapter,
    redis: AgentRedis,
    storage: Storage,
):
    """Conduct a measurement."""
    measurement_agent = request.measurement_agent

    logger.info("Create local measurement directory")
    measurement_results_path = (
        settings.AGENT_RESULTS_DIR_PATH / measurement_agent.measurement_uuid
    )
    measurement_results_path.mkdir(exist_ok=True)

    probes_filepath = None
    results_filepath = measurement_results_path / results_key(
        measurement_agent.agent_uuid, request.round
    )

    logger.info("Download CSV probe file locally")
    probes_filepath = await storage.download_file_to(
        storage.measurement_bucket(measurement_agent.measurement_uuid),
        request.probe_filename,
        settings.AGENT_TARGETS_DIR_PATH,
    )

    logger.info("User ID: %s", measurement_agent.measurement.user_id)
    logger.info("Probe File: %s", request.probe_filename)
    logger.info(request.round)
    logger.info("Tool: %s", measurement_agent.measurement.tool)
    logger.info("Tool Parameters: %s", measurement_agent.tool_parameters)
    logger.info("Max Probing Rate: %s", measurement_agent.probing_rate)

    probing_start_time = datetime.utcnow()
    with Manager() as manager:
        prober_statistics = manager.dict()  # type: ignore

        prober_process = Process(
            target=probe,
            args=(
                settings,
                results_filepath,
                request.round.number,
                measurement_agent.probing_rate,
                prober_statistics,
                probes_filepath,
            ),
        )

        prober_process.start()
        is_not_canceled = await watcher(
            prober_process, settings, measurement_agent.measurement_uuid, redis
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
            measurement_agent.measurement_uuid, measurement_agent.agent_uuid, statistics
        )

        logger.info("Upload results file into S3")
        await storage.upload_file(
            storage.measurement_bucket(measurement_agent.measurement_uuid),
            results_key(measurement_agent.agent_uuid, request.round),
            results_filepath,
        )
    else:
        logger.warning("Measurement canceled")

    if not settings.AGENT_DEBUG_MODE:
        logger.info("Empty local results directory")
        shutil.rmtree(settings.AGENT_RESULTS_DIR_PATH)
        settings.AGENT_RESULTS_DIR_PATH.mkdir()

        logger.info("Empty local targets directory")
        shutil.rmtree(settings.AGENT_TARGETS_DIR_PATH)
        settings.AGENT_TARGETS_DIR_PATH.mkdir()

        if request.probe_filename:
            logger.info("Remove prefix file from S3")
            await storage.soft_delete(
                storage.measurement_bucket(measurement_agent.measurement_uuid),
                request.probe_filename,
            )
