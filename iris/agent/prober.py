"""Prober executor."""

from iris.agent import logger
from iris.agent.settings import AgentSettings
from iris.commons.subprocess import start_stream_subprocess


settings = AgentSettings()


async def probe(
    parameters,
    result_filepath,
    starttime_filepath,
    csv_filepath=None,
    target_filepath=None,
):
    """Execute measurement with Iris."""
    cmd = (
        str(settings.AGENT_D_MINER_PROBER_PATH)
        + " -o "
        + str(result_filepath)
        + " -r "
        + str(settings.AGENT_PROBING_RATE)
        + " --buffer-sniffer-size="
        + str(settings.AGENT_BUFFER_SNIFFER_SIZE)
        + " -d "
        + str(settings.AGENT_IPS_PER_SUBNET)
        + " -i "
        + str(settings.AGENT_INF_BORN)
        + " -s "
        + str(settings.AGENT_SUP_BORN)
        + " -p "
        + str(parameters["parameters"]["protocol"])
        + " --dport="
        + str(parameters["parameters"]["destination_port"])
        + " --min-ttl="
        + str(parameters["parameters"]["min_ttl"])
        + " --max-ttl="
        + str(parameters["parameters"]["max_ttl"])
        + " -E "
        + str(settings.AGENT_EXCLUSION_FILE_PATH)
        + " --record-timestamp "
        + " --start-time-log-file="
        + str(starttime_filepath)
    )
    if parameters["round"] == 1:
        if target_filepath is not None:
            cmd += " -T -t " + str(target_filepath)
    elif csv_filepath is not None:
        cmd += " -F -f " + str(csv_filepath)
    else:
        logger.error("Executable Invalid parameters")
        return

    logger.info(cmd)

    await start_stream_subprocess(cmd, logger=logger)
