from uuid import uuid4

import pytest

from iris.api.targets import verify_probe_target_file, verify_target_file
from iris.commons.models.pagination import Paginated
from iris.commons.models.target import Target, TargetSummary
from tests.assertions import assert_response, assert_status_code, cast_response
from tests.helpers import FakeUploadFile, upload_file


async def test_get_targets_probing_not_enabled(make_client, make_user):
    client = make_client(make_user(probing_enabled=False))
    assert_status_code(client.get("/targets"), 403)


async def test_get_targets_empty(make_client, make_user, storage):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    await storage.create_bucket(storage.targets_bucket(str(user.id)))
    assert_response(
        client.get("/targets"), Paginated[TargetSummary](count=0, results=[])
    )


async def test_get_targets(make_client, make_user, make_tmp_file, storage):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    bucket = storage.targets_bucket(str(user.id))
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)
    result = cast_response(client.get("/targets"), Paginated[TargetSummary])
    assert result.count == 1
    assert result.results[0].key == tmp_file["name"]


async def test_get_target(make_client, make_user, make_tmp_file, storage):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    bucket = storage.targets_bucket(str(user.id))
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)
    result = cast_response(client.get(f"/targets/{tmp_file['name']}"), Target)
    assert result.key == tmp_file["name"]
    assert result.content == tmp_file["content"].splitlines()
    assert result.size == len(tmp_file["content"])


async def test_get_target_not_found(make_client, make_user, storage):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    bucket = storage.targets_bucket(str(user.id))
    await storage.create_bucket(bucket)
    assert_status_code(client.get(f"/targets/{uuid4()}"), 404)


async def test_delete_target(make_client, make_user, make_tmp_file, storage):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    bucket = storage.targets_bucket(str(user.id))
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)
    assert_status_code(client.delete(f"/targets/{tmp_file['name']}"), 204)


async def test_delete_target_not_found(make_client, make_user, storage):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    await storage.create_bucket(storage.targets_bucket(str(user.id)))
    assert_status_code(client.delete(f"/targets/{uuid4()}"), 404)


async def test_post_target(make_client, make_user, storage, tmp_path):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    await storage.create_bucket(storage.targets_bucket(str(user.id)))
    filepath = tmp_path / "targets.csv"
    filepath.write_text("1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20")
    with filepath.open("rb") as f:
        response = client.post("/targets/", files={"target_file": f})
    assert_status_code(response, 201)
    target = cast_response(response, Target)
    assert target.content == []
    assert target.key == "targets.csv"


async def test_post_target_invalid_extension(make_client, make_user, storage, tmp_path):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    await storage.create_bucket(storage.targets_bucket(str(user.id)))
    filepath = tmp_path / "targets.txt"
    filepath.write_text("1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20")
    with filepath.open("rb") as f:
        response = client.post("/targets/", files={"target_file": f})
    assert_status_code(response, 412)


async def test_post_target_invalid_content(make_client, make_user, storage, tmp_path):
    user = make_user(probing_enabled=True)
    client = make_client(user)
    await storage.create_bucket(storage.targets_bucket(str(user.id)))
    filepath = tmp_path / "targets.csv"
    filepath.write_text("abcd")
    with filepath.open("rb") as f:
        response = client.post("/targets/", files={"target_file": f})
    assert_status_code(response, 412)


async def test_post_probes(make_client, make_user, storage, tmp_path):
    user = make_user(probing_enabled=True, is_superuser=True)
    client = make_client(user)
    await storage.create_bucket(storage.targets_bucket(str(user.id)))
    filepath = tmp_path / "probes.csv"
    filepath.write_text("8.8.8.8,24000,0,32,icmp")
    with filepath.open("rb") as f:
        response = client.post("/targets/probes", files={"target_file": f})
    assert_status_code(response, 201)
    target = cast_response(response, Target)
    assert target.content == []
    assert target.key == "probes.csv"


async def test_post_probes_invalid_extension(make_client, make_user, storage, tmp_path):
    user = make_user(probing_enabled=True, is_superuser=True)
    client = make_client(user)
    await storage.create_bucket(storage.targets_bucket(str(user.id)))
    filepath = tmp_path / "probes.txt"
    filepath.write_text("8.8.8.8,24000,0,32,icmp")
    with filepath.open("rb") as f:
        response = client.post("/targets/probes", files={"target_file": f})
    assert_status_code(response, 412)


async def test_post_probes_invalid_content(make_client, make_user, storage, tmp_path):
    user = make_user(probing_enabled=True, is_superuser=True)
    client = make_client(user)
    await storage.create_bucket(storage.targets_bucket(str(user.id)))
    filepath = tmp_path / "probes.csv"
    filepath.write_text("abcd")
    with filepath.open("rb") as f:
        response = client.post("/targets/probes", files={"target_file": f})
    assert_status_code(response, 412)


@pytest.mark.parametrize(
    "content",
    [
        "1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20",
        "1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20\n",  # Trailing line
    ],
)
def test_verify_target_file_valid(content):
    assert verify_target_file(FakeUploadFile(content))


@pytest.mark.parametrize(
    "content",
    [
        "",  # Empty file
        "1.1.1.1\ntest\n2.2.2.0/24",  # Invalid lines
        "2.2.2.0/24,tcp,5,20",  # Invalid protocol
        "2.2.2.0/24,icmp,test,20",  # Invalid min TTL
        "2.2.2.0/24,icmp,-5,20",  # Invalid min TTL
        "2.2.2.0/24,icmp,2,test",  # Invalid max TTL
        "2.2.2.0/24,icmp,2,300",  # Invalid max TTL
        "2.2.2.0/24,udp,5,20\n\n",  # Trailing lines
    ],
)
def test_verify_target_file_invalid(content):
    assert not verify_target_file(FakeUploadFile(content))


@pytest.mark.parametrize(
    "content",
    [
        "8.8.8.8,24000,33434,32,icmp\n2001:4860:4860::8888,24000,33434,32,icmp",
        "8.8.8.8,24000,0,32,icmp",  # Port 0
    ],
)
def test_verify_probe_target_file_valid(content):
    assert verify_probe_target_file(FakeUploadFile(content))


@pytest.mark.parametrize(
    "content",
    [
        "",  # Empty file
        "8.8.453.8,24000,33434,32,icmp",  # Invalid destination address
        "8.8.8.8,24000,33434,32,icmt",  # Invalid protocol
        "8.8.8.8,-1,33434,32,icmp",  # Invalid source port
        "8.8.8.8,65536,33434,32,icmp",  # Invalid source port
        "8.8.8.8,24000,-1,32,icmp",  # Invalid destination port
        "8.8.8.8,24000,65536,32,icmp",  # Invalid destination port
        "8.8.8.8,24000,33434,-1,icmp",  # Invalid TTL
        "8.8.8.8,24000,33434,256,icmp",  # Invalid TTL
    ],
)
def test_verify_probe_target_file_invalid(content):
    assert not verify_probe_target_file(FakeUploadFile(content))
