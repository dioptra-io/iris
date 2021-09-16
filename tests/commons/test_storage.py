import logging

import pytest

from iris.commons.storage import Storage


@pytest.mark.asyncio
async def test_buckets(common_settings):
    storage = Storage(common_settings, logging.getLogger(__name__))

    assert await storage.create_bucket("bucket1") is None
    # Creating the same bucket twice should not fail
    assert await storage.create_bucket("bucket2") is None
    assert await storage.create_bucket("bucket2") is None
    # "Infrastructure" buckets must be ignored
    assert await storage.create_bucket("targets") is None
    assert await storage.get_measurement_buckets() == ["bucket1", "bucket2"]

    assert await storage.delete_bucket("bucket1") is None
    assert await storage.delete_bucket("bucket2") is None
    assert await storage.get_measurement_buckets() == []


@pytest.mark.asyncio
async def test_files(common_settings, tmp_path):
    storage = Storage(common_settings, logging.getLogger(__name__))
    await storage.create_bucket("bucket1")
    await storage.create_bucket("bucket2")

    content = "1234"
    metadata = {"meta": "data"}
    tmp_file = tmp_path / "file.txt"
    tmp_file.write_text(content)

    # Upload
    with tmp_file.open("rb") as f:
        await storage.upload_file_no_retry("bucket1", "1.txt", f, metadata)
    await storage.upload_file("bucket1", "2.txt", tmp_file, metadata)

    # List
    assert len(await storage.get_all_files_no_retry("bucket1")) == 2

    # Get
    for key in ["1.txt", "2.txt"]:
        file = await storage.get_file_no_retry("bucket1", key)
        assert file["key"] == key
        assert file["content"] == content
        assert file["metadata"] == metadata

    # Download
    download_file = tmp_path / "download.txt"
    await storage.download_file("bucket1", "1.txt", download_file)
    assert download_file.read_text() == content

    # Copy
    await storage.copy_file_to_bucket("bucket1", "bucket2", "1.txt", "1.txt")
    file1 = await storage.get_file_no_retry("bucket1", "1.txt")
    file2 = await storage.get_file_no_retry("bucket2", "1.txt")
    # TODO: metadata are not copied currently, is this a bug?
    # ["content", "key", "metadat]
    for key in ["content", "key"]:
        assert file1[key] == file2[key]

    # Deletion
    await storage.delete_file_check_no_retry("bucket1", "1.txt")
    with pytest.raises(Exception):
        await storage.delete_file_check_no_retry("bucket1", "1.txt")

    await storage.delete_file_no_check("bucket1", "2.txt")
    await storage.delete_file_no_check("bucket1", "2.txt")

    assert len(await storage.get_all_files_no_retry("bucket1")) == 0
    assert len(await storage.get_all_files_no_retry("bucket2")) == 1

    await storage.delete_all_files_from_bucket("bucket2")
    assert len(await storage.get_all_files_no_retry("bucket2")) == 0
