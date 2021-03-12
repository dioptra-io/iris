"""Prober executor."""

import asyncio

from iris.commons.subprocess import CancelProcessException, start_stream_subprocess


async def stopper(settings, redis, measurement_uuid, logger, logger_prefix=""):
    """Cancel prober conditions."""
    while True:
        measurement_state = await redis.get_measurement_state(measurement_uuid)
        if measurement_state is None:
            logger.warning(logger_prefix + "Measurement canceled")
            raise CancelProcessException
        await asyncio.sleep(settings.WORKER_STOPPER_REFRESH)


async def probe(
    settings,
    parameters,
    results_filepath,
    logger,
    stdin=None,
    prefix_incl_filepath=None,
    probes_filepath=None,
    stopper=None,
    logger_prefix="",
    n_packets=None,
):
    """Execute measurement with Diamond-Miner."""
    cmd = (
        str(settings.AGENT_D_MINER_PROBER_PATH)
        + " --output-file-csv "
        + str(results_filepath)
        + " --probing-rate "
        + str(parameters["probing_rate"])
        + " --protocol "
        + str(parameters["protocol"])
        + " --sniffer-buffer-size="
        + str(100_000)  # TODO: removed in the next prober version
        + " --filter-min-ttl="
        + str(parameters["min_ttl"])
        + " --filter-max-ttl="
        + str(parameters["max_ttl"])
        + " --meta-round="
        + str(parameters["round"])
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

    if n_packets:
        cmd += f" --n-packets={n_packets}"

    logger.info(logger_prefix + cmd)

    return await start_stream_subprocess(
        cmd,
        stdout=logger.info,
        stdin=stdin,  # In case of exhaustive round or targets-list input
        stderr=logger.warning,
        stopper=stopper,
        prefix=logger_prefix,
    )
