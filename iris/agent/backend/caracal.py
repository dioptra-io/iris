import asyncio
import os
import shlex
import signal
from asyncio.subprocess import create_subprocess_shell
from logging import LoggerAdapter
from pathlib import Path

from iris.agent.settings import AgentSettings
from iris.commons.models import MeasurementRoundRequest
from iris.commons.redis import Redis


async def caracal_backend(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    logger: LoggerAdapter,
    redis: Redis,
    probes_filepath: Path,
    results_filepath: Path,
) -> dict | None:
    """
    This is the default and reference backend for Iris.
    It uses `caracal <https://github.com/dioptra-io/caracal>`_ for sending the probes.
    """

    prober = asyncio.create_task(
        probe(
            settings,
            logger,
            probes_filepath,
            results_filepath,
            request.round.number,
            request.batch_size,
            request.probing_rate,
        )
    )

    watcher = asyncio.create_task(
        watch_cancellation(
            redis,
            request.measurement_uuid,
            settings.AGENT_UUID,
            settings.AGENT_STOPPER_REFRESH,
        )
    )

    done, pending = await asyncio.wait(
        [prober, watcher], return_when=asyncio.FIRST_COMPLETED
    )
    if watcher in done:
        # Measurement was cancelled
        prober.cancel()
        return None

    return prober.result()


async def watch_cancellation(
    redis: Redis,
    measurement_uuid: str,
    agent_uuid: str,
    interval: float,
) -> bool:
    """Kill the prober process if the measurement request is deleted."""
    while True:
        if not await redis.get_request(measurement_uuid, agent_uuid):
            return True
        await asyncio.sleep(interval)


async def probe(
    settings: AgentSettings,
    logger: LoggerAdapter,
    probes_filepath: Path,
    results_filepath: Path,
    round_number: int,
    batch_size: int | None,
    probing_rate: int,
) -> dict:
    """Probing interface."""
    # Cap the probing rate if superior to the maximum probing rate
    measurement_probing_rate = (
        probing_rate
        if probing_rate and probing_rate <= settings.AGENT_MAX_PROBING_RATE
        else settings.AGENT_MAX_PROBING_RATE
    )

    if probes_filepath.suffix == ".zst":
        input_cmd = f"zstd -cd {shlex.quote(str(probes_filepath))}"
    else:
        input_cmd = f"cat {shlex.quote(str(probes_filepath))}"

    if results_filepath.suffix == ".zst":
        output_cmd = f"zstd -c > {shlex.quote(str(results_filepath))}"
    else:
        output_cmd = f"tee > {shlex.quote(str(results_filepath))}"

    caracal_cmd = [
        "caracal",
        f"--meta-round {shlex.quote(str(round_number))}",
        f"--probing-rate {shlex.quote(str(measurement_probing_rate))}",
        f"--sniffer-wait-time {settings.AGENT_CARACAL_SNIFFER_WAIT_TIME}",
    ]

    if batch_size:
        caracal_cmd.append(f"--batch-size {shlex.quote(str(batch_size))}")

    if exclude_path := settings.AGENT_CARACAL_EXCLUDE_PATH:
        caracal_cmd.append(
            f"--filter-from-prefix-file-excl {shlex.quote(str(exclude_path))}"
        )

    if not settings.AGENT_CARACAL_INTEGRITY_CHECK:
        caracal_cmd.append("--no-integrity-check")

    cmd = f"{input_cmd} | {' '.join(caracal_cmd)} | {output_cmd}"
    logger.info("Running %s", cmd)

    process = await create_subprocess_shell(cmd, preexec_fn=os.setsid)
    try:
        await process.wait()
    except asyncio.CancelledError:
        logger.info("Terminating pid %s", process.pid)
        os.killpg(os.getpgid(process.pid), signal.SIGKILL)

    # These statistics have been lost when migrating from pycaracal to caracal.
    # TODO: Re-implement them.
    return {
        "probes_read": 0,
        "packets_sent": 0,
        "packets_failed": 0,
        "filtered_low_ttl": 0,
        "filtered_high_ttl": 0,
        "filtered_prefix_excl": 0,
        "filtered_prefix_not_incl": 0,
        "packets_received": 0,
        "packets_received_invalid": 0,
        "pcap_received": 0,
        "pcap_dropped": 0,
        "pcap_interface_dropped": 0,
    }
