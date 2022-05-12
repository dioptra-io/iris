import asyncio
from collections import defaultdict
from io import TextIOWrapper
from ipaddress import IPv6Address
from logging import LoggerAdapter
from pathlib import Path
from typing import Iterable, List

from httpx import AsyncClient
from zstandard import ZstdDecompressor

from iris.agent.settings import AgentSettings
from iris.commons.models import MeasurementRoundRequest
from iris.commons.redis import Redis

ATLAS_PROTOCOLS = {"icmp": "ICMP", "icmp6": "ICMP", "udp": "UDP"}


async def atlas_inner_pipeline(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    logger: LoggerAdapter,
    redis: Redis,
    probes_filepath: Path,
    results_filepath: Path,
):
    # TODO: Test, test min/max_ttl
    # TODO: convert results to Iris format
    # TODO: pretty inefficient to rebuild the target list from the probes,
    # it would be better to directly get the target list file here.
    logger.info("Converting probes to RIPE Atlas targets")
    with probes_filepath.open("rb") as f:
        ctx = ZstdDecompressor()
        with ctx.stream_reader(f) as stream:
            targets = probes_to_targets(TextIOWrapper(stream))

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
        base_url="https://atlas.ripe.net/api/v2/",
        params=dict(key=settings.AGENT_RIPE_ATLAS_KEY),
        timeout=30,
    ) as client:
        logger.info("Creating RIPE Atlas measurements")
        group_id = await create_measurement_group(client, definitions)
        logger.info("Watching RIPE Atlas measurements")
        while True:
            group_status = await get_measurement_group_status(client, group_id)
            logger.info("RIPE Atlas group status: %s", group_status)
            # (a) Stop when all the measurements are done.
            if all(x == "Stopped" for x in group_status):
                cancelled = False
                break
            # (b) Stop if the measurement request was cancelled.
            if not await redis.get_request(
                request.measurement_uuid, settings.AGENT_UUID
            ):
                cancelled = True
                break
            await asyncio.sleep(10)  # TODO: Parametrize the refresh interval?

    if not cancelled:
        logger.info("Fetching RIPE Atlas results")
        # TODO

    probing_statistics = dict(
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

    return probing_statistics, not cancelled


def probes_to_targets(lines: Iterable[str]) -> dict:
    """
    >>> probes_to_targets([
    ...     "::ffff:192.0.2.1,24000,33434,1,icmp",
    ...     "::ffff:192.0.2.1,24000,33435,4,icmp",
    ...     "::ffff:192.0.2.2,24000,33434,1,icmp",
    ... ])
    {('::ffff:192.0.2.1', 'icmp'): (2, 1, 4), ('::ffff:192.0.2.2', 'icmp'): (1, 1, 1)}
    """
    targets = defaultdict(lambda: (set(), set()))
    for line in lines:
        dst_addr, src_port, dst_port, ttl, protocol = line.strip().split(",")
        dst_addr = IPv6Address(dst_addr)
        dst_addr = str(dst_addr.ipv4_mapped) if dst_addr.ipv4_mapped else str(dst_addr)
        targets[(dst_addr, protocol)][0].add((int(src_port), int(dst_port)))
        targets[(dst_addr, protocol)][1].add(int(ttl))
    return {
        k: (len(flows), min(ttls), max(ttls)) for k, (flows, ttls) in targets.items()
    }


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


async def create_measurement_group(client: AsyncClient, definitions: List[dict]) -> int:
    r = await client.post(
        "/measurements",
        json=dict(
            definitions=definitions,
            probes=[dict(requested=1, type="area", value="WW")],
        ),
    )
    data = r.json()
    return data["measurements"][0]


async def get_measurement_group_status(client: AsyncClient, group_id: int) -> List[str]:
    data = (await client.get(f"/measurements/groups/{group_id}")).json()
    return [
        await get_measurement_status(client, member["id"])
        for member in data["group_members"]
    ]


async def get_measurement_status(client: AsyncClient, measurement_id: int) -> str:
    data = (await client.get(f"/measurements/{measurement_id}")).json()
    return data["status"]["name"]


async def stop_measurement_group(client: AsyncClient, group_id: int) -> None:
    await client.delete(f"/measurements/groups/{group_id}")
