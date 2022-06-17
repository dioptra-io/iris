import pytest
from httpx import AsyncClient

from iris.agent.backend.atlas import (
    ATLAS_BASE_URL,
    get_measurement_group_members,
    get_measurement_group_results,
    get_measurement_group_status,
    get_measurement_results,
    get_measurement_status,
)

MEASUREMENT_ID = 41808650
"""https://atlas.ripe.net/measurements/41808650/"""


@pytest.fixture
async def atlas_client():
    async with AsyncClient(base_url=ATLAS_BASE_URL, timeout=5) as client:
        yield client


async def collect(it):
    xs = []
    async for x in it:
        xs.append(x)
    return xs


async def test_get_measurement_group_members(atlas_client):
    members = await get_measurement_group_members(atlas_client, MEASUREMENT_ID)
    assert members == [41808650, 41808651, 41808652, 41808653, 41808654, 41808655]


async def test_get_measurement_results(atlas_client):
    data = await collect(get_measurement_results(atlas_client, MEASUREMENT_ID))
    assert len(data) == 1
    assert data[0]["msm_id"] == MEASUREMENT_ID


async def test_get_measurement_group_results(atlas_client):
    data = await collect(get_measurement_group_results(atlas_client, MEASUREMENT_ID))
    assert len(data) == 6


async def test_get_measurement_status(atlas_client):
    status = await get_measurement_status(atlas_client, MEASUREMENT_ID)
    assert status == "Stopped"


async def test_get_measurement_group_status(atlas_client):
    status = await get_measurement_group_status(atlas_client, MEASUREMENT_ID)
    assert status == ["Stopped", "Stopped", "Stopped", "Stopped", "Stopped", "Stopped"]
