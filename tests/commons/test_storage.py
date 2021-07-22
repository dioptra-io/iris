from io import BytesIO

import pytest

from iris.commons.storage import Storage


@pytest.mark.asyncio
async def test_storage(common_settings):
    storage = Storage(settings=common_settings)

    assert await storage.create_bucket("bucket1") is None
    assert await storage.create_bucket("bucket2") is None
    assert await storage.get_measurement_buckets() == ["bucket1", "bucket2"]

    file = BytesIO(b"1234")
    await storage.upload_file_no_retry(
        "bucket1", "1234.txt", file, metadata={"meta": "data"}
    )

    file = await storage.get_file_no_retry("bucket1", "1234.txt")
    assert file["key"] == "1234.txt"
    assert file["content"] == "1234"
    assert file["metadata"] == {"meta": "data"}

    await storage.delete_file_check_no_retry("bucket1", "1234.txt")
    with pytest.raises(Exception):
        await storage.delete_file_check_no_retry("bucket1", "1234.txt")

    assert await storage.delete_bucket("bucket1") is None
    assert await storage.delete_bucket("bucket2") is None
    assert await storage.get_measurement_buckets() == []
