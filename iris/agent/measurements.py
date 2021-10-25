"""Measurement interface."""
import shutil
from datetime import datetime
from logging import Logger
from multiprocessing import Manager, Process

from iris.agent.probe_generator import build_probe_generator_parameters
from iris.agent.prober import probe, watcher
from iris.agent.settings import AgentSettings
from iris.commons.redis import AgentRedis
from iris.commons.schemas.private import MeasurementRoundRequest
from iris.commons.schemas.public import ProbingStatistics, Tool
from iris.commons.storage import Storage, results_key, targets_key


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

    gen_parameters = None
    probes_filepath = None
    results_filepath = measurement_results_path / results_key(agent.uuid, request.round)

    # A) A probe file is specified, download it.
    # This is usually the case for round > 1.
    if request.probe_filename:
        logger.info(f"{logger_prefix} Download CSV probe file locally")
        probes_filepath = await storage.download_file_to(
            storage.measurement_bucket(measurement_request.uuid),
            request.probe_filename,
            settings.AGENT_TARGETS_DIR_PATH,
        )

    # HACK: A') The tool is "Probes".
    # We directly pass it to caracal.
    elif measurement_request.tool == Tool.Probes:
        logger.info(f"{logger_prefix} Download CSV probe file locally")
        probes_filepath = await storage.download_file_to(
            storage.archive_bucket(measurement_request.username),
            targets_key(measurement_request.uuid, agent.uuid),
            settings.AGENT_TARGETS_DIR_PATH,
        )

    # B) No probe file is specified, generate probes.
    # This is usually the case for for round = 1.
    else:
        logger.info(f"{logger_prefix} Download target file locally")
        target_filepath = await storage.download_file_to(
            storage.archive_bucket(measurement_request.username),
            targets_key(measurement_request.uuid, agent.uuid),
            settings.AGENT_TARGETS_DIR_PATH,
        )

        prefix_filepath = None
        if request.prefix_filename:
            logger.info(f"{logger_prefix} Download CSV prefix file locally")
            prefix_filepath = await storage.download_file_to(
                storage.measurement_bucket(measurement_request.uuid),
                request.prefix_filename,
                settings.AGENT_TARGETS_DIR_PATH,
            )

        logger.info(f"{logger_prefix} Build probe generator parameters")
        gen_parameters = build_probe_generator_parameters(
            settings.AGENT_MIN_TTL,
            request.round,
            measurement_request.tool,
            agent.tool_parameters,
            open(target_filepath),
            open(prefix_filepath) if prefix_filepath else None,
        )

    logger.info(f"{logger_prefix} Username : {measurement_request.username}")
    logger.info(f"{logger_prefix} Target File: {agent.target_file}")
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
                gen_parameters,
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

        if request.prefix_filename:
            logger.info(f"{logger_prefix} Remove probe file from S3")
            await storage.soft_delete(
                storage.measurement_bucket(measurement_request.uuid),
                request.prefix_filename,
            )

        if request.probe_filename:
            logger.info(f"{logger_prefix} Remove prefix file from S3")
            await storage.soft_delete(
                storage.measurement_bucket(measurement_request.uuid),
                request.probe_filename,
            )
