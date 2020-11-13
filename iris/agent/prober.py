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
    csv_filepath=None,
    target_filepath=None,
    target_type=None,
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
        + " --buffer-sniffer-size="
        + str(settings.AGENT_BUFFER_SNIFFER_SIZE)
        + " -d "
        + str(settings.AGENT_IPS_PER_SUBNET)
        + " -i "
        + str(settings.AGENT_INF_BORN)
        + " -s "
        + str(settings.AGENT_SUP_BORN)
        + " -p "
        + str(parameters["protocol"])
        + " --dport="
        + str(parameters["destination_port"])
        + " --min-ttl="
        + str(parameters["min_ttl"])
        + " --max-ttl="
        + str(parameters["max_ttl"])
        + " -E "
        + str(settings.AGENT_EXCLUSION_FILE_PATH)
        + " --record-timestamp"
        + " --experimental-host-offset"
        + " --start-time-log-file="
        + str(starttime_filepath)
    )
    if parameters["round"] == 1:
        if target_filepath is not None:
            if target_type == "prefixes-list":
                # `prefixes-list` target file
                cmd += " -P --prefix-file=" + str(target_filepath)
            else:
                # `targets-list` target file
                cmd += " -T -t " + str(target_filepath)
    elif csv_filepath is not None:
        cmd += " -F -f " + str(csv_filepath)
    else:
        logger.error(logger_prefix + "Invalid executable parameters")
        return

    logger.info(logger_prefix + cmd)

    return await start_stream_subprocess(
        cmd,
        stdout=logger.info,
        stderr=logger.warning,
        stopper=stopper,
        prefix=logger_prefix,
    )
