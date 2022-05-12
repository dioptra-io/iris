from collections import defaultdict
from io import TextIOWrapper
from logging import LoggerAdapter
from pathlib import Path

from httpx import AsyncClient
from zstandard import ZstdDecompressor

from iris.agent.settings import AgentSettings
from iris.commons.models import MeasurementRoundRequest
from iris.commons.redis import Redis


async def atlas_inner_pipeline(
    settings: AgentSettings,
    request: MeasurementRoundRequest,
    logger: LoggerAdapter,
    redis: Redis,
    probes_filepath: Path,
    results_filepath: Path,
):
    # TODO: watch cancellation
    # TODO: convert results to Iris format
    # TODO: pretty inefficient to rebuild the target list from the probes,
    # it would be better to directly get the target list file here.
    logger.info("Converting probes to RIPE Atlas targets")
    targets = defaultdict(lambda: (set(), set()))
    with probes_filepath.open("rb") as f:
        ctx = ZstdDecompressor()
        with ctx.stream_reader(f) as stream:
            lines = TextIOWrapper(stream)
            for line in lines:
                dst_addr, src_port, dst_port, ttl, protocol = line.split(",")
                targets[(dst_addr, protocol)][0].add((int(src_port), int(dst_port)))
                targets[(dst_addr, protocol)][1].add(int(ttl))

    async with AsyncClient(
        base_url="https://atlas.ripe.net/api/v2/",
        params=dict(key=settings.AGENT_RIPE_ATLAS_KEY),
    ) as client:
        for (dst_addr, protocol), (flows, ttls) in targets.items():
            if dst_addr.startswith("::ffff:"):
                dst_addr = dst_addr[7:]
            await create_measurement(
                client=client,
                target=dst_addr,
                protocol=protocol,
                first_hop=min(ttls),
                max_hops=max(ttls),
                paris=len(flows),
                measurement_uuid=request.measurement_uuid,
            )


async def create_measurement(
    client: AsyncClient,
    target: str,
    protocol: str,
    first_hop: int,
    max_hops: int,
    paris: int,
    measurement_uuid: str,
):
    # TODO: Create multiple measurements at once?
    # TODO: Use groups feature?
    await client.post(
        "/measurements",
        json=[
            dict(
                af=4 if "." in target else 6,
                description=f"iris:{measurement_uuid}",
                first_hop=first_hop,
                max_hops=max_hops,
                is_oneoff=True,
                is_public=False,
                paris=paris,
                protocol=protocol,
                start_time="NOW",  # TODO
                tags=f"iris,iris:{measurement_uuid}",
                target=target,
                type="traceroute",
            )
        ],
    )
    # TODO: return id


async def stop_measurement(client: AsyncClient, identifier: int):
    await client.delete(f"/measurements/{identifier}")
