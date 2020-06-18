from diamond_miner.commons.subprocess import start_stream_subprocess
from diamond_miner.worker import logger
from diamond_miner.worker.settings import WorkerSettings


settings = WorkerSettings()


async def pcap_to_csv(
    round_number, result_filepath, starttime_filepath, output_csv_filepath, parameters
):
    """Transform a PCAP & start time log file into CSV using D-Miner Reader."""
    # Snapshot numbering is currently unused
    snapshot_number = 1

    cmd = (
        str(settings.WORKER_D_MINER_READER_PATH)
        + " -r "
        + " -i "
        + str(result_filepath)
        + " -o "
        + str(output_csv_filepath)
        + " -E "
        + str(settings.WORKER_EXCLUSION_FILE_PATH)
        + " -R "
        + str(round_number)
        + " -s "
        + str(snapshot_number)
        + " --dport="
        + str(parameters["destination_port"])
        + " --compute-rtt "
        + " --start-time-log-file="
        + str(starttime_filepath)
    )

    await start_stream_subprocess(cmd, logger=logger)
