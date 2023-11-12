import pytest
from fastapi import HTTPException

from iris.api.validator import target_file_validator
from iris.commons.models import Tool, ToolParameters
from tests.helpers import create_user_buckets, upload_target_file


async def test_probes_not_found(make_user, storage):
    user = make_user(is_superuser=True)
    with pytest.raises(HTTPException) as e:
        await target_file_validator(
            storage, Tool.Probes, ToolParameters(), user, "targets.csv", 24, 64
        )
    assert "file not found" in e.value.detail


async def test_probes_not_superuser(make_user, storage):
    user = make_user()
    await create_user_buckets(storage, user)
    await upload_target_file(storage, user, "targets.csv", is_probes_file=True)
    with pytest.raises(HTTPException) as e:
        await target_file_validator(
            storage, Tool.Probes, ToolParameters(), user, "targets.csv", 24, 64
        )
    assert "privileges required" in e.value.detail


async def test_probes_not_probes(make_user, storage):
    user = make_user(is_superuser=True)
    await create_user_buckets(storage, user)
    await upload_target_file(storage, user, "targets.csv")
    with pytest.raises(HTTPException) as e:
        await target_file_validator(
            storage, Tool.Probes, ToolParameters(), user, "targets.csv", 24, 64
        )
    assert "not a probe file" in e.value.detail


async def test_probes(make_user, storage):
    user = make_user(is_superuser=True)
    await create_user_buckets(storage, user)
    await upload_target_file(storage, user, "targets.csv", is_probes_file=True)
    ttl = await target_file_validator(
        storage, Tool.Probes, ToolParameters(), user, "targets.csv", 24, 64
    )
    assert ttl == (0, 255)


async def test_targets_not_found(make_user, storage):
    user = make_user()
    with pytest.raises(HTTPException) as e:
        await target_file_validator(
            storage, Tool.DiamondMiner, ToolParameters(), user, "targets.csv", 24, 64
        )
    assert "file not found" in e.value.detail


async def test_targets_missing_columns(make_user, storage):
    user = make_user()
    await create_user_buckets(storage, user)
    await upload_target_file(
        storage, user, "targets.csv", content=["0.0.0.0/24,icmp,8,32"]
    )
    with pytest.raises(HTTPException) as e:
        await target_file_validator(
            storage, Tool.Ping, ToolParameters(), user, "targets.csv", 24, 64
        )
    assert "Invalid line" in e.value.detail


async def test_targets_invalid_prefix_length(make_user, storage):
    user = make_user()
    await create_user_buckets(storage, user)
    await upload_target_file(
        storage, user, "targets.csv", content=["0.0.0.0/25,icmp,8,32,6"]
    )
    with pytest.raises(HTTPException) as e:
        await target_file_validator(
            storage, Tool.Ping, ToolParameters(), user, "targets.csv", 24, 64
        )
    assert "prefixes length" in e.value.detail


async def test_targets_ping_udp(make_user, storage):
    user = make_user()
    await create_user_buckets(storage, user)
    await upload_target_file(
        storage, user, "targets.csv", content=["0.0.0.0/24,udp,8,32,6"]
    )
    with pytest.raises(HTTPException) as e:
        await target_file_validator(
            storage, Tool.Ping, ToolParameters(), user, "targets.csv", 24, 64
        )
    assert "only accessible with ICMP protocol" in e.value.detail


async def test_targets(make_user, storage):
    user = make_user()
    await create_user_buckets(storage, user)
    await upload_target_file(
        storage, user, "targets.csv", content=["0.0.0.0/24,icmp,8,32,6"]
    )
    ttl = await target_file_validator(
        storage, Tool.Ping, ToolParameters(), user, "targets.csv", 24, 64
    )
    assert ttl == (8, 32)
