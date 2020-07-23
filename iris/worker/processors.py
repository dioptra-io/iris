import ipaddress

from iris.commons.subprocess import start_stream_subprocess
from iris.worker import logger
from iris.worker.settings import WorkerSettings


settings = WorkerSettings()


async def pcap_to_csv(
    round_number, result_filepath, starttime_filepath, csv_filepath, parameters
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
        + str(csv_filepath)
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


async def next_round_csv(
    round_number, table_name, csv_filepath, agent_parameters, measurement_parameters
):
    """Compute the next round and output CSV file."""
    # Snapshot numbering is currently unused
    snapshot_number = 1

    cmd = (
        str(settings.WORKER_D_MINER_READER_PATH)
        + " -g  -o "
        + csv_filepath
        + " -R "
        + str(round_number)
        + " -s "
        + str(snapshot_number)
        + " -t"
        + table_name
        + " --db-host="
        + str(settings.DATABASE_HOST)
        + " -v "
        + str(int(ipaddress.IPv4Address(agent_parameters["ip_address"])))
        + " --dport="
        + str(measurement_parameters["destination_port"])
        + " --encoded-ttl-from=udp-length"
        # Not curently used
        # + " --skip-prefixes="
        # + options.heartbeat_dir
        # + "resources/"
        # + options.remote_probe_hostname
        # + "_skip_prefix "
    )

    await start_stream_subprocess(cmd, logger=logger)


async def shuffle_next_round_csv(csv_filepath, shuffled_csv_filepath):
    cmd = (
        "export MEMORY="
        + str(settings.WORKER_TERASHUF_MEMORY)
        + "; export TMPDIR="
        + str(settings.WORKER_TERASHUF_TMP_DIR_PATH)
        + "; "
        + str(settings.WORKER_TERASHUF_PATH)
        + " < "
        + csv_filepath
        + " > "
        + shuffled_csv_filepath
    )

    await start_stream_subprocess(cmd, logger=logger)
