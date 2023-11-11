"""Measurement interface."""
import shutil
from datetime import datetime
from logging import LoggerAdapter

from iris.agent.backend import caracal_backend
from iris.agent.settings import AgentSettings
from iris.commons.models import MeasurementRoundRequest, ProbingStatistics
from iris.commons.redis import Redis
from iris.commons.storage import Storage, results_key


async def outer_pipeline(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    logger: LoggerAdapter,
    redis: Redis,
    storage: Storage,
):
    """Conduct a measurement."""
    logger.info("Launch measurement procedure")

    logger.info("Create local measurement directory")
    measurement_results_path = (
        settings.AGENT_RESULTS_DIR_PATH / request.measurement_uuid
    )
    measurement_results_path.mkdir(exist_ok=True)

    results_filepath = measurement_results_path / results_key(request.round)

    logger.info("Download CSV probe file locally")
    probes_filepath = await storage.download_file_to(
        storage.measurement_agent_bucket(request.measurement_uuid, settings.AGENT_UUID),
        request.probe_filename,
        settings.AGENT_TARGETS_DIR_PATH,
    )

    logger.info("Probe file: %s", request.probe_filename)
    logger.info("%s", request.round)
    logger.info("Requested probing rate: %s", request.probing_rate)

    probing_start_time = datetime.utcnow()
    prober_statistics = await caracal_backend(
        settings, request, logger, redis, probes_filepath, results_filepath
    )

    if prober_statistics:
        logger.info("Upload probing statistics to Redis")
        statistics = ProbingStatistics(
            round=request.round,
            start_time=probing_start_time,
            end_time=datetime.utcnow(),
            **prober_statistics,
        )
        await redis.set_measurement_stats(
            request.measurement_uuid, settings.AGENT_UUID, statistics
        )

        logger.info("Upload results file into S3")
        await storage.upload_file(
            storage.measurement_agent_bucket(
                request.measurement_uuid, settings.AGENT_UUID
            ),
            results_key(request.round),
            results_filepath,
        )
    else:
        logger.warning("Measurement canceled")

    logger.info("Empty local results directory")
    shutil.rmtree(settings.AGENT_RESULTS_DIR_PATH)
    settings.AGENT_RESULTS_DIR_PATH.mkdir()

    logger.info("Empty local targets directory")
    shutil.rmtree(settings.AGENT_TARGETS_DIR_PATH)
    settings.AGENT_TARGETS_DIR_PATH.mkdir()
