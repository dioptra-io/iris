import asyncio
import json
from collections import defaultdict
from collections.abc import AsyncIterator, Iterable, Iterator
from ipaddress import IPv6Address
from logging import LoggerAdapter
from pathlib import Path
from typing import BinaryIO

from httpx import AsyncClient

from iris.agent.settings import AgentSettings
from iris.commons.models import MeasurementRoundRequest
from iris.commons.redis import Redis
from iris.commons.utils import zstd_stream_reader_text, zstd_stream_writer

ATLAS_BASE_URL = "https://atlas.ripe.net/api/v2/"
ATLAS_PROTOCOLS = {"icmp": "ICMP", "icmp6": "ICMP", "tcp": "TCP", "udp": "UDP"}
IRIS_PROTOCOLS = {"ICMP": 1, "TCP": 6, "UDP": 17}


async def atlas_backend(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    logger: LoggerAdapter,
    redis: Redis,
    probes_filepath: Path,
    results_filepath: Path,
) -> dict | None:
    """
    This is an experimental backend using `RIPE Atlas <https://atlas.ripe.net/>`_ to send the probes.
    This gives access to a large number of vantage points, at the expense of very low speed probing.
    """
    logger.info("Converting probes to RIPE Atlas targets")
    with zstd_stream_reader_text(probes_filepath) as f:
        targets = group_probes(f)

    definitions = [
        make_definition(
            measurement_uuid=request.measurement_uuid,
            dst_addr=dst_addr,
            protocol=protocol,
            min_ttl=min_ttl,
            max_ttl=max_ttl,
            n_flows=n_flows,
        )
        for (dst_addr, protocol), (n_flows, min_ttl, max_ttl) in targets.items()
    ]

    async with AsyncClient(
        base_url=ATLAS_BASE_URL,
        params=dict(key=settings.AGENT_RIPE_ATLAS_KEY),
        timeout=30,
    ) as client:
        logger.info("Creating RIPE Atlas measurements")
        group_id = await create_measurement_group(client, definitions)
        logger.info("Watching RIPE Atlas measurements (group %s)", group_id)
        cancelled = await watch_measurement_group(
            client,
            logger,
            redis,
            request.measurement_uuid,
            settings.AGENT_UUID,
            group_id,
        )
        if cancelled:
            return None
        logger.info("Fetching RIPE Atlas results")
        with zstd_stream_writer(results_filepath) as f:
            await fetch_measurement_group_results(
                client, group_id, request.round.number, f
            )
        return dict(
            probes_read=0,
            packets_sent=0,
            packets_failed=0,
            filtered_low_ttl=0,
            filtered_high_ttl=0,
            filtered_prefix_excl=0,
            filtered_prefix_not_incl=0,
            packets_received=0,
            packets_received_invalid=0,
            pcap_received=0,
            pcap_dropped=0,
            pcap_interface_dropped=0,
        )  # TODO


def group_probes(lines: Iterable[str]) -> dict:
    """
    Group probes by destination address and protocol in order to minimize the number of measurements required.
    >>> group_probes([
    ...     "::ffff:192.0.2.1,24000,33434,1,icmp",
    ...     "::ffff:192.0.2.1,24000,33435,4,icmp",
    ...     "::ffff:192.0.2.2,24000,33434,1,icmp",
    ... ])
    {('192.0.2.1', 'icmp'): (2, 1, 4), ('192.0.2.2', 'icmp'): (1, 1, 1)}
    """
    targets = defaultdict(lambda: (set(), set()))
    for line in lines:
        dst_addr, src_port, dst_port, ttl, protocol = line.strip().split(",")
        dst_addr_v6 = IPv6Address(dst_addr)
        dst_addr = (
            str(dst_addr_v6.ipv4_mapped)
            if dst_addr_v6.ipv4_mapped
            else str(dst_addr_v6)
        )
        targets[(dst_addr, protocol)][0].add((int(src_port), int(dst_port)))
        targets[(dst_addr, protocol)][1].add(int(ttl))
    return {
        k: (len(flows), min(ttls), max(ttls)) for k, (flows, ttls) in targets.items()
    }


def traceroute_to_replies(traceroute: dict, round_: int) -> Iterator[tuple]:
    # TODO: Handle IPv6/ICMPv6.
    for hop in traceroute["result"]:
        for result in hop["result"]:
            if "from" in result:
                yield (
                    traceroute[
                        "timestamp"
                    ],  # capture_timestamp - not available with RIPE Atlas.
                    IRIS_PROTOCOLS[traceroute["proto"]],  # probe_protocol
                    "::ffff:" + traceroute["src_addr"],  # probe_src_addr
                    "::ffff:" + traceroute["dst_addr"],  # probe_dst_addr
                    traceroute["paris_id"],  # probe_src_port
                    traceroute["paris_id"],  # probe_dst_port
                    hop["hop"],  # probe_ttl
                    0,  # quoted_ttl - not available with RIPE Atlas.
                    "::ffff:" + result["from"],  # reply_src_addr
                    1,  # reply_protocol
                    11,  # reply_icmp_type - TODO: echo reply vs time exceeded vs dest unreachable
                    0,  # TODO: reply_icmp_code
                    result["ttl"],  # reply_ttl
                    result["size"],  # reply_size
                    [],  # TODO: reply_mpls_labels
                    int(result["rtt"] * 10),  # rtt (tenth of ms)
                    round_,  # round
                )


def make_definition(
    measurement_uuid: str,
    dst_addr: str,
    protocol: str,
    min_ttl: int,
    max_ttl: int,
    n_flows: int,
) -> dict:
    return dict(
        af=4 if "." in dst_addr else 6,
        description=measurement_uuid,
        first_hop=min_ttl,
        max_hops=max_ttl,
        is_oneoff=True,
        is_public=False,
        packets=1,
        paris=n_flows,
        protocol=ATLAS_PROTOCOLS[protocol],
        response_timeout=1000,
        tags=["iris"],
        target=dst_addr,
        type="traceroute",
    )


async def create_measurement_group(client: AsyncClient, definitions: list[dict]) -> int:
    r = await client.post(
        "/measurements",
        json=dict(
            definitions=definitions,
            probes=[dict(requested=1, type="area", value="WW")],
        ),
    )
    data = r.json()
    return data["measurements"][0]


async def get_measurement_group_members(
    client: AsyncClient, group_id: int
) -> list[int]:
    data = (await client.get(f"/measurements/groups/{group_id}")).json()
    return [member["id"] for member in data["group_members"]]


async def get_measurement_group_results(
    client: AsyncClient, group_id: int
) -> AsyncIterator[dict]:
    members = await get_measurement_group_members(client, group_id)
    for member in members:
        async for result in get_measurement_results(client, member):
            yield result


async def get_measurement_results(
    client: AsyncClient, measurement_id: int
) -> AsyncIterator[dict]:
    async with client.stream(
        "GET", f"/measurements/{measurement_id}/results", params=dict(format="txt")
    ) as stream:
        async for line in stream.aiter_lines():
            yield json.loads(line)


async def get_measurement_group_status(client: AsyncClient, group_id: int) -> list[str]:
    members = await get_measurement_group_members(client, group_id)
    return [await get_measurement_status(client, member) for member in members]


async def get_measurement_status(client: AsyncClient, measurement_id: int) -> str:
    data = (await client.get(f"/measurements/{measurement_id}")).json()
    return data["status"]["name"]


async def stop_measurement_group(client: AsyncClient, group_id: int) -> None:
    await client.delete(f"/measurements/groups/{group_id}")


async def watch_measurement_group(
    client: AsyncClient,
    logger: LoggerAdapter,
    redis: Redis,
    measurement_uuid: str,
    agent_uuid: str,
    group_id: int,
    *,
    refresh_interval: int = 10,
) -> bool:
    group_status = await get_measurement_group_status(client, group_id)
    while not all(x == "Stopped" for x in group_status):
        group_status = await get_measurement_group_status(client, group_id)
        logger.info("RIPE Atlas group status: %s", group_status)
        # Stop if the measurement request was cancelled.
        if not await redis.get_request(measurement_uuid, agent_uuid):
            await stop_measurement_group(client, group_id)
            return True
        await asyncio.sleep(refresh_interval)
    return False


async def fetch_measurement_group_results(
    client: AsyncClient, group_id: int, round: int, writer: BinaryIO
) -> None:
    async for result in get_measurement_group_results(client, group_id):
        for reply in traceroute_to_replies(result, round):
            writer.write(",".join(str(x) for x in reply).encode() + b"\n")
