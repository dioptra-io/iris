"""Measurement interface."""
import shutil
from datetime import datetime
from logging import LoggerAdapter
from multiprocessing import Manager, Process

from iris.agent.prober import probe, watch_cancellation
from iris.agent.settings import AgentSettings
from iris.commons.models import MeasurementRoundRequest, ProbingStatistics
from iris.commons.redis import Redis
from iris.commons.storage import Storage, results_key


async def do_measurement(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    logger: LoggerAdapter,
    redis: Redis,
    storage: Storage,
):
    """Conduct a measurement."""
    logger.info("Launch measurement procedure")
    measurement = request.measurement
    measurement_agent = request.measurement_agent
    assert measurement_agent.measurement_uuid  # make mypy happy

    logger.info("Create local measurement directory")
    measurement_results_path = (
        settings.AGENT_RESULTS_DIR_PATH / measurement_agent.measurement_uuid
    )
    measurement_results_path.mkdir(exist_ok=True)

    results_filepath = measurement_results_path / results_key(request.round)

    logger.info("Download CSV probe file locally")
    probes_filepath = await storage.download_file_to(
        storage.measurement_agent_bucket(
            measurement_agent.measurement_uuid, measurement_agent.agent_uuid
        ),
        request.probe_filename,
        settings.AGENT_TARGETS_DIR_PATH,
    )

    logger.info("User ID: %s", measurement.user_id)
    logger.info("Probe File: %s", request.probe_filename)
    logger.info("%s", request.round)
    logger.info("Tool: %s", measurement.tool)
    logger.info("Tool Parameters: %s", measurement_agent.tool_parameters)
    logger.info("Max Probing Rate: %s", measurement_agent.probing_rate)

    probing_start_time = datetime.utcnow()
    with Manager() as manager:
        prober_statistics = manager.dict()  # type: ignore

        prober_process = Process(
            target=probe,
            args=(
                settings,
                probes_filepath,
                results_filepath,
                request.round.number,
                measurement_agent.probing_rate,
                prober_statistics,
            ),
        )

        prober_process.start()
        is_not_canceled = await watch_cancellation(
            redis,
            prober_process,
            measurement_agent.measurement_uuid,
            measurement_agent.agent_uuid,
            settings.AGENT_STOPPER_REFRESH,
        )
        prober_statistics = dict(prober_statistics)

    if is_not_canceled:
        logger.info("Upload probing statistics to Redis")
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
            storage.measurement_agent_bucket(
                measurement_agent.measurement_uuid, measurement_agent.agent_uuid
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
