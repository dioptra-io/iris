"""Prober executor."""

import asyncio
from multiprocessing import Process
from pathlib import Path
from typing import Dict, Optional
from uuid import UUID

from pycaracal import prober, set_log_level

from iris.agent.settings import AgentSettings
from iris.commons.redis import AgentRedis
from iris.commons.schemas.public import MeasurementState


async def watcher(
    process: Process, settings: AgentSettings, measurement_uuid: UUID, redis: AgentRedis
) -> bool:
    """Watch the prober execution and stop it according to the measurement state."""
    while process.is_alive():
        measurement_state = await redis.get_measurement_state(measurement_uuid)
        if measurement_state in [
            MeasurementState.Canceled,
            MeasurementState.Unknown,
        ]:
            process.kill()
            return False
        await asyncio.sleep(settings.AGENT_STOPPER_REFRESH)
    return True


def probe(
    settings: AgentSettings,
    results_filepath: Path,
    round_number: int,
    probing_rate: int,
    prober_statistics: Dict,
    probes_filepath: Optional[Path] = None,
) -> None:
    """Probing interface."""
    # Cap the probing rate if superior to the maximum probing rate
    measurement_probing_rate = (
        probing_rate
        if probing_rate and probing_rate <= settings.AGENT_MAX_PROBING_RATE
        else settings.AGENT_MAX_PROBING_RATE
    )

    # This set the log level of the C++ logger (spdlog).
    # This allow the logs to be filtered in C++ (fast)
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
