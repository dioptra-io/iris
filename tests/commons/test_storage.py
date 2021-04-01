import abc

import aioboto3
import pytest

from iris.commons.settings import CommonSettings
from iris.commons.storage import Storage


class BaseFakeBotoDriver(abc.ABC):
    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    async def __aenter__(self, *args, **kwargs):
        pass

    async def __aexit__(self, *args, **kwargs):
        pass


# Test of buckets methods


@pytest.mark.asyncio
async def test_storage_get_measurement_bucket(monkeypatch):
    # Test when buckets are present
    class FakeBotoClient(object):
        async def list_buckets(*args, **kwargs):
            return {"Buckets": [{"Name": "bucket1"}, {"Name": "bucket2"}]}

    class FakeBotoDriver(BaseFakeBotoDriver):
        async def __aenter__(self, *args, **kwargs):
            return FakeBotoClient()

    monkeypatch.setattr(aioboto3, "client", FakeBotoDriver)

    storage = Storage(settings=CommonSettings())
    assert await storage.get_measurement_buckets() == ["bucket1", "bucket2"]

    class FakeBotoClientEmpty(object):
        async def list_buckets(*args, **kwargs):
            return {"Buckets": []}

    class FakeBotoDriver(BaseFakeBotoDriver):
        async def __aenter__(self, *args, **kwargs):
            return FakeBotoClientEmpty()

    monkeypatch.setattr(aioboto3, "client", FakeBotoDriver)

    storage = Storage(settings=CommonSettings())
    assert await storage.get_measurement_buckets() == []


@pytest.mark.asyncio
async def test_storage_create_bucket(monkeypatch):
    class FakeBotoClient(object):
        async def create_bucket(*args, **kwargs):
            return None

    class FakeBotoDriver(BaseFakeBotoDriver):
        async def __aenter__(self, *args, **kwargs):
            return FakeBotoClient()

    monkeypatch.setattr(aioboto3, "client", FakeBotoDriver)

    storage = Storage(settings=CommonSettings())
    assert await storage.create_bucket("bucket") is None


@pytest.mark.asyncio
async def test_storage_delete_bucket(monkeypatch):
    class FakeBotoClient(object):
        async def delete_bucket(*args, **kwargs):
            return None

    class FakeBotoDriver(BaseFakeBotoDriver):
        async def __aenter__(self, *args, **kwargs):
            return FakeBotoClient()

    monkeypatch.setattr(aioboto3, "client", FakeBotoDriver)

    storage = Storage(settings=CommonSettings())
    assert await storage.delete_bucket("bucket") is None


# Test of delete files methods


@pytest.mark.asyncio
async def test_storage_delete_file(monkeypatch):
    class FakeBotoClient(object):
        async def delete_object(*args, **kwargs):
            return None

    class FakeBotoDriver(BaseFakeBotoDriver):
        async def __aenter__(self, *args, **kwargs):
            return FakeBotoClient()

    monkeypatch.setattr(aioboto3, "client", FakeBotoDriver)

    storage = Storage(settings=CommonSettings())
    assert await storage.delete_file_no_check("bucket", "file") is None
