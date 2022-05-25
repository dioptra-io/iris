import asyncio
from logging import LoggerAdapter
from multiprocessing import Manager, Process
from pathlib import Path

from pycaracal import prober, set_log_level

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
    with Manager() as manager:
        probing_statistics = manager.dict()  # type: ignore
        prober_process = Process(
            target=probe,
            args=(
                settings,
                probes_filepath,
                results_filepath,
                request.round.number,
                request.batch_size,
                request.probing_rate,
                probing_statistics,
            ),
        )
        prober_process.start()
        cancelled = await watch_cancellation(
            redis,
            prober_process,
            request.measurement_uuid,
            settings.AGENT_UUID,
            settings.AGENT_STOPPER_REFRESH,
        )
        probing_statistics = dict(probing_statistics)

    return None if cancelled else probing_statistics


async def watch_cancellation(
    redis: Redis,
    process: Process,
    measurement_uuid: str,
    agent_uuid: str,
    interval: float,
) -> bool:
    """Kill the prober process if the measurement request is deleted."""
    while process.is_alive():
        if not await redis.get_request(measurement_uuid, agent_uuid):
            process.kill()
            return True
        await asyncio.sleep(interval)
    return False


def probe(
    settings: AgentSettings,
    probes_filepath: Path,
    results_filepath: Path,
    round_number: int,
    batch_size: int | None,
    probing_rate: int,
    probing_statistics: dict,
) -> None:
    """Probing interface."""
    # Cap the probing rate if superior to the maximum probing rate
    measurement_probing_rate = (
        probing_rate
        if probing_rate and probing_rate <= settings.AGENT_MAX_PROBING_RATE
        else settings.AGENT_MAX_PROBING_RATE
    )

    # This set the log level of the C++ logger (spdlog).
    # This allows the logs to be filtered in C++ (fast)
    # before being forwarded to the (slower) Python logger.
    set_log_level(settings.AGENT_CARACAL_LOGGING_LEVEL)

    # Prober configuration
    config = prober.Config()
    config.set_output_file_csv(str(results_filepath))

    config.set_probing_rate(measurement_probing_rate)
    config.set_rate_limiting_method(settings.AGENT_CARACAL_RATE_LIMITING_METHOD.value)
    config.set_sniffer_wait_time(settings.AGENT_CARACAL_SNIFFER_WAIT_TIME)
    config.set_integrity_check(settings.AGENT_CARACAL_INTEGRITY_CHECK)
    config.set_meta_round(str(round_number))

    if batch_size:
        config.set_batch_size(batch_size)

    if settings.AGENT_CARACAL_EXCLUDE_PATH is not None:
        config.set_prefix_excl_file(str(settings.AGENT_CARACAL_EXCLUDE_PATH))

    prober_stats, sniffer_stats, pcap_stats = prober.probe(config, str(probes_filepath))

    # Populate the statistics
    # TODO: Implement __dict__ in pycaracal.
    probing_statistics["probes_read"] = prober_stats.read
    probing_statistics["packets_sent"] = prober_stats.sent
    probing_statistics["packets_failed"] = prober_stats.failed
    probing_statistics["filtered_low_ttl"] = prober_stats.filtered_lo_ttl
    probing_statistics["filtered_high_ttl"] = prober_stats.filtered_hi_ttl
    probing_statistics["filtered_prefix_excl"] = prober_stats.filtered_prefix_excl
    probing_statistics[
        "filtered_prefix_not_incl"
    ] = prober_stats.filtered_prefix_not_incl

    probing_statistics["packets_received"] = sniffer_stats.received_count
    probing_statistics[
        "packets_received_invalid"
    ] = sniffer_stats.received_invalid_count
    probing_statistics["pcap_received"] = pcap_stats.received
    probing_statistics["pcap_dropped"] = pcap_stats.dropped
    probing_statistics["pcap_interface_dropped"] = pcap_stats.interface_dropped
