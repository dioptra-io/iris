"""Prober executor."""

import asyncio

from iris.agent import logger
from iris.agent.settings import AgentSettings
from iris.commons.subprocess import start_stream_subprocess


settings = AgentSettings()


async def stopper(logger, redis, measurement_uuid, logger_prefix=""):
    """Cancel prober conditions."""
    while True:
        measurement_state = await redis.get_measurement_state(measurement_uuid)
        if measurement_state is None:
            logger.warning(logger_prefix + "Measurement canceled")
            raise Exception
        await asyncio.sleep(settings.WORKER_STOPPER_REFRESH)


async def probe(
    parameters,
    result_filepath,
    starttime_filepath,
    stdin=None,
    prefix_incl_filepath=None,
    probes_filepath=None,
    stopper=None,
    logger_prefix="",
):
    """Execute measurement with Diamond-Miner."""
    cmd = (
        str(settings.AGENT_D_MINER_PROBER_PATH)
        + " -o "
        + str(result_filepath)
        + " -r "
        + str(parameters["probing_rate"])
        + " --sniffer-buffer-size="
        + str(settings.AGENT_BUFFER_SNIFFER_SIZE)
        + " -p "
        + str(parameters["protocol"])
        + " --filter-min-ttl="
        + str(parameters["min_ttl"])
        + " --filter-max-ttl="
        + str(parameters["max_ttl"])
        + " --start-time-log-file="
        + str(starttime_filepath)
    )

    if settings.AGENT_DEBUG_MODE:
        cmd += " --log-level=trace"

    # In case of prefixes-list input
    if prefix_incl_filepath is not None:
        cmd += f" --filter-from-prefix-file-incl={prefix_incl_filepath}"

    # Excluded prefixes
    if settings.AGENT_D_MINER_EXCLUDE_PATH is not None:
        cmd += f" --filter-from-prefix-file-excl={settings.AGENT_D_MINER_EXCLUDE_PATH}"

    # Probes file for round > 0
    if probes_filepath is not None:
        cmd += f" --input-file={probes_filepath}"

    if probes_filepath and stdin:
        logger.error("Cannot pass `probes_filepath` and `stdin` at the same time")
        return

    logger.info(logger_prefix + cmd)

    return await start_stream_subprocess(
        cmd,
        stdout=logger.info,
        stdin=stdin,  # In case of exhaustive round or targets-list input
        stderr=logger.warning,
        stopper=stopper,
        prefix=logger_prefix,
    )
