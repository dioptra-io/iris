import tempfile

import pytest

from iris.api.targets import verify_target_file

# --- GET /api/targets ---


def test_get_targets(api_client_sync, monkeypatch):
    class FakeStorage(object):
        async def get_all_files_no_retry(*args, **kwargs):
            return [
                {
                    "key": "test",
                    "size": 42,
                    "content": "1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20",
                    "last_modified": "test",
                    "metadata": None,
                }
            ]

    api_client_sync.app.storage = FakeStorage()

    response = api_client_sync.get("/api/targets")
    assert response.json() == {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [{"key": "test", "last_modified": "test"}],
    }


def test_get_targets_empty(api_client_sync, monkeypatch):
    class FakeStorage(object):
        async def get_all_files_no_retry(*args, **kwargs):
            return []

    api_client_sync.app.storage = FakeStorage()

    response = api_client_sync.get("/api/targets")
    assert response.json() == {
        "count": 0,
        "next": None,
        "previous": None,
        "results": [],
    }


# --- GET /api/targets/{key} ---


def test_get_targets_by_key(api_client_sync, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            return {
                "key": "test",
                "size": 42,
                "content": "1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20",
                "last_modified": "test",
                "metadata": None,
            }

    api_client_sync.app.storage = FakeStorage()

    response = api_client_sync.get("/api/targets/test")
    assert response.json() == {
        "key": "test",
        "size": 42,
        "content": ["1.1.1.0/24,icmp,2,32", "2.2.2.0/24,udp,5,20"],
        "last_modified": "test",
    }


def test_get_targets_by_key_not_found(api_client_sync, monkeypatch):
    class FakeStorage(object):
        async def get_file_no_retry(*args, **kwargs):
            raise Exception

    api_client_sync.app.storage = FakeStorage()

    response = api_client_sync.get("/api/targets/test")
    assert response.status_code == 404


# --- POST /api/targets ---


@pytest.mark.asyncio
async def test_verify_prefixes_list_file():
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
    assert await verify_target_file(file_container) is False

    # Test with adhequate file
    file_container.register(b"1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20")
    assert await verify_target_file(file_container) is True

    # Test with inadhequate file
    file_container.register(b"1.1.1.1\ntest\n2.2.2.0/24")
    assert await verify_target_file(file_container) is False

    # Test with bad protocol
    file_container.register(b"1.1.1.0/24,icmp,2,32\n2.2.2.0/24,tcp,5,20")
    assert await verify_target_file(file_container) is False

    # Test with bad ttl
    file_container.register(b"1.1.1.0/24,icmp,2,32\n2.2.2.0/24,icmp,test,20")
    assert await verify_target_file(file_container) is False

    # Test with bad ttl
    file_container.register(b"1.1.1.0/24,icmp,2,32\n2.2.2.0/24,icmp,2,test")
    assert await verify_target_file(file_container) is False

    # Test with invalid ttl
    file_container.register(b"1.1.1.0/24,icmp,2,32\n2.2.2.0/24,icmp,test,500")
    assert await verify_target_file(file_container) is False

    # Test with invalid ttl
    file_container.register(b"1.1.1.0/24,icmp,2,32\n2.2.2.0/24,icmp,0,test")
    assert await verify_target_file(file_container) is False

    # Test with invalid ttl
    file_container.register(b"1.1.1.0/24,icmp,2,32\n2.2.2.0/24,icmp,-5,test")
    assert await verify_target_file(file_container) is False

    # Test with adhequate file with one trailing lines
    file_container.register(b"1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20\n")
    assert await verify_target_file(file_container) is True

    # Test with adhequate file with multiple trailing lines
    file_container.register(b"1.1.1.0/24,icmp,2,32\n2.2.2.0/24,udp,5,20\n\n")
    assert await verify_target_file(file_container) is False


# --- DELETE /api/targets/{key} ---


def test_delete_targets_by_key(api_client_sync, monkeypatch):
    class FakeStorage(object):
        async def delete_file_check_no_retry(*args, **kwargs):
            return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    api_client_sync.app.storage = FakeStorage()

    response = api_client_sync.delete("/api/targets/test")
    assert response.json() == {"key": "test", "action": "delete"}


def test_delete_targets_by_key_not_found(api_client_sync, monkeypatch):
    class FakeStorage(object):
        async def delete_file_check_no_retry(*args, **kwargs):
            raise Exception

    api_client_sync.app.storage = FakeStorage()

    response = api_client_sync.delete("/api/targets/test")
    assert response.status_code == 404


def test_delete_targets_internal_error(api_client_sync, monkeypatch):
    class FakeStorage(object):
        async def delete_file_check_no_retry(*args, **kwargs):
            return {"ResponseMetadata": {"HTTPStatusCode": 500}}

    api_client_sync.app.storage = FakeStorage()

    response = api_client_sync.delete("/api/targets/test")
    assert response.status_code == 500
