import asyncio
from multiprocessing import Manager, Process
from pathlib import Path
from typing import Dict

from pycaracal import prober, set_log_level

from iris.agent.settings import AgentSettings
from iris.commons.models import MeasurementRoundRequest
from iris.commons.redis import Redis


async def caracal_inner_pipeline(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    redis: Redis,
    probes_filepath: Path,
    results_filepath: Path,
):
    with Manager() as manager:
        prober_statistics = manager.dict()  # type: ignore
        prober_process = Process(
            target=probe,
            args=(
                settings,
                probes_filepath,
                results_filepath,
                request.round.number,
                request.probing_rate,
                prober_statistics,
            ),
        )
        prober_process.start()
        is_not_canceled = await watch_cancellation(
            redis,
            prober_process,
            request.measurement_uuid,
            settings.AGENT_UUID,
            settings.AGENT_STOPPER_REFRESH,
        )
        prober_statistics = dict(prober_statistics)

    return prober_statistics, is_not_canceled


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
            return False
        await asyncio.sleep(interval)
    return True


def probe(
    settings: AgentSettings,
    probes_filepath: Path,
    results_filepath: Path,
    round_number: int,
    probing_rate: int,
    prober_statistics: Dict,
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

    if settings.AGENT_CARACAL_EXCLUDE_PATH is not None:
        config.set_prefix_excl_file(str(settings.AGENT_CARACAL_EXCLUDE_PATH))

    prober_stats, sniffer_stats, pcap_stats = prober.probe(config, str(probes_filepath))

    # Populate the statistics
    prober_statistics["probes_read"] = prober_stats.read
    prober_statistics["packets_sent"] = prober_stats.sent
    prober_statistics["packets_failed"] = prober_stats.failed
    prober_statistics["filtered_low_ttl"] = prober_stats.filtered_lo_ttl
    prober_statistics["filtered_high_ttl"] = prober_stats.filtered_hi_ttl
    prober_statistics["filtered_prefix_excl"] = prober_stats.filtered_prefix_excl
    prober_statistics[
        "filtered_prefix_not_incl"
    ] = prober_stats.filtered_prefix_not_incl

    prober_statistics["packets_received"] = sniffer_stats.received_count
    prober_statistics["packets_received_invalid"] = sniffer_stats.received_invalid_count
    prober_statistics["pcap_received"] = pcap_stats.received
    prober_statistics["pcap_dropped"] = pcap_stats.dropped
    prober_statistics["pcap_interface_dropped"] = pcap_stats.interface_dropped
