"""Test of `targets` operations."""

import tempfile

import pytest

from iris.api.targets import verify_quota, verify_targets_file

# --- GET /v0/targets ---


def test_get_targets(client, monkeypatch):
    """Get all targets key."""

    class FakeStorage(object):
        async def get_all_files_no_retry(*args, **kwargs):
            return [
                {
                    "key": "test",
                    "size": 42,
                    "last_modified": "test",
                    "metadata": None,
                }
            ]

    client.app.storage = FakeStorage()

    response = client.get("/v0/targets")
    assert response.json() == {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [{"key": "test", "size": 42, "last_modified": "test"}],
    }


def test_get_targets_empty(client, monkeypatch):
    """Get all targets key when empty."""

    class FakeStorage(object):
        async def get_all_files_no_retry(*args, **kwargs):
            return []

    client.app.storage = FakeStorage()

    response = client.get("/v0/targets")
    assert response.json() == {
        "count": 0,
        "next": None,
        "previous": None,
        "results": [],
    }


# --- GET /v0/targets/{key} ---


def test_get_targets_by_key(client, monkeypatch):
    """Test get targets file by key."""

    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test",
                "size": 42,
                "last_modified": "test",
                "metadata": None,
            }

    client.app.storage = FakeStorage()

    response = client.get("/v0/targets/test")
    assert response.json() == {
        "key": "test",
        "size": 42,
        "last_modified": "test",
    }


def test_get_targets_by_key_not_found(client, monkeypatch):
    """Test get targets file by key."""

    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            raise Exception

    client.app.storage = FakeStorage()

    response = client.get("/v0/targets/test")
    assert response.status_code == 404


# --- POST /v0/targets ---


@pytest.mark.asyncio
async def test_verify_prefixes_list_file():
    """Test file verification."""

    class FileContainer(object):
        def __init__(self):
            self.file = tempfile.SpooledTemporaryFile()

        def register(self, content):
            self.file = tempfile.SpooledTemporaryFile()
            self.file.write(content)
            self.file.seek(0)

    file_container = FileContainer()

    # Test with empty file
    file_container.register(b"")
    assert await verify_targets_file(file_container) is False

    # Test with adhequate file
    file_container.register(b"1.1.1.0/24\n2.2.2.0/24")
    assert await verify_targets_file(file_container) is True

    # Test with inadhequate file
    file_container.register(b"1.1.1.1\ntest\n2.2.2.0/24")
    assert await verify_targets_file(file_container) is False

    # Test with invalid prefix length
    file_container.register(b"1.1.1.0/25\n2.2.2.0/24")
    assert await verify_targets_file(file_container) is False

    # Test with adhequate file with one trailing lines
    file_container.register(b"1.1.1.0/24\n2.2.2.0/24\n")
    assert await verify_targets_file(file_container) is True

    # Test with adhequate file with multiple trailing lines
    file_container.register(b"1.1.1.0/24\n2.2.2.0/24\n\n")
    assert await verify_targets_file(file_container) is False


@pytest.mark.asyncio
async def test_verify_quota():
    """Test quota verification."""

    class FileContainer(object):
        def __init__(self):
            self.file = tempfile.SpooledTemporaryFile()

        def register(self, content):
            self.file = tempfile.SpooledTemporaryFile()
            self.file.write(content)
            self.file.seek(0)

    file_container = FileContainer()

    file_container.register(b"1.1.1.0/24\n2.2.2.0/24")

    assert await verify_quota(file_container, 1) is False
    assert await verify_quota(file_container, 2) is True
    assert await verify_quota(file_container, 3) is True


# --- DELETE /v0/targets/{key} ---


def test_delete_targets_by_key(client, monkeypatch):
    """Test deelte targets file by key."""

    class FakeStorage(object):
        async def delete_file_check_no_retry(*args, **kwargs):
            return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    client.app.storage = FakeStorage()

    response = client.delete("/v0/targets/test")
    assert response.json() == {"key": "test", "action": "delete"}


def test_delete_targets_by_key_not_found(client, monkeypatch):
    """Test get targets file by key."""

    class FakeStorage(object):
        async def delete_file_check_no_retry(*args, **kwargs):
            raise Exception

    client.app.storage = FakeStorage()

    response = client.delete("/v0/targets/test")
    assert response.status_code == 404


def test_delete_targets_internal_error(client, monkeypatch):
    """Test deelte targets file by key."""

    class FakeStorage(object):
        async def delete_file_check_no_retry(*args, **kwargs):
            return {"ResponseMetadata": {"HTTPStatusCode": 500}}

    client.app.storage = FakeStorage()

    response = client.delete("/v0/targets/test")
    assert response.status_code == 500
