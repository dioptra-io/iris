import logging
from uuid import uuid4

import pytest

from iris.commons.storage import Storage
from tests.helpers import upload_file

pytestmark = pytest.mark.asyncio


async def test_create_bucket(storage, make_bucket):
    bucket = make_bucket()
    await storage.create_bucket(bucket)
    assert bucket in await storage.get_measurement_buckets()


async def test_create_bucket_twice(storage, make_bucket):
    # Creating the same bucket twice should not fail
    bucket = make_bucket()
    await storage.create_bucket(bucket)
    await storage.create_bucket(bucket)
    assert bucket in await storage.get_measurement_buckets()


async def test_delete_bucket(storage, make_bucket):
    bucket = make_bucket()
    await storage.create_bucket(bucket)
    await storage.delete_bucket(bucket)
    assert bucket not in await storage.get_measurement_buckets()


async def test_upload_file(storage, make_bucket, make_tmp_file):
    bucket = make_bucket()
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)

    file = await storage.get_file(bucket, tmp_file["name"])
    assert file["content"] == tmp_file["content"]
    assert file["metadata"] == tmp_file["metadata"]
    assert file["key"] == tmp_file["name"]
    assert file["size"] == len(tmp_file["content"])


async def test_download_file(storage, make_bucket, make_tmp_file, tmp_path):
    bucket = make_bucket()
    tmp_file = make_tmp_file()
    download_path = tmp_path / str(uuid4())
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)
    await storage.download_file(bucket, tmp_file["name"], download_path)
    assert download_path.read_text() == tmp_file["content"]


async def test_download_file_to(storage, make_bucket, make_tmp_file, tmp_path):
    bucket = make_bucket()
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)
    download_path = await storage.download_file_to(bucket, tmp_file["name"], tmp_path)
    assert download_path.read_text() == tmp_file["content"]


async def test_copy_file(storage, make_bucket, make_tmp_file):
    bucket_1 = make_bucket()
    bucket_2 = make_bucket()
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket_1)
    await storage.create_bucket(bucket_2)
    await upload_file(storage, bucket_1, tmp_file)
    await storage.copy_file_to_bucket(
        bucket_1, bucket_2, tmp_file["name"], tmp_file["name"]
    )
    file_1 = await storage.get_file(bucket_1, tmp_file["name"])
    file_2 = await storage.get_file(bucket_2, tmp_file["name"])
    # TODO: metadata are not copied currently, is this a bug?
    # ["content", "key", "metadata"]
    for key in ["content", "key"]:
        assert file_1[key] == file_2[key]


async def test_get_all_files(storage, make_bucket, make_tmp_file):
    bucket = make_bucket()
    await storage.create_bucket(bucket)

    tmp_files = [make_tmp_file(), make_tmp_file()]
    tmp_files = sorted(tmp_files, key=lambda x: x["path"])

    for tmp_file in tmp_files:
        await upload_file(storage, bucket, tmp_file)

    files = await storage.get_all_files(bucket)
    files = sorted(files, key=lambda x: x["key"])

    assert len(files) == len(tmp_files)
    for file, tmp_file in zip(files, tmp_files):
        assert file["metadata"] == tmp_file["metadata"]
        assert file["key"] == tmp_file["name"]
        assert file["size"] == len(tmp_file["content"])


async def test_delete_file_check(storage, make_bucket, make_tmp_file):
    bucket = make_bucket()
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)
    await storage.delete_file_check_no_retry(bucket, tmp_file["name"])
    assert len(await storage.get_all_files(bucket)) == 0


async def test_delete_file_check_not_found(storage, make_bucket, make_tmp_file):
    bucket = make_bucket()
    await storage.create_bucket(bucket)
    with pytest.raises(Exception):
        await storage.delete_file_check_no_retry(bucket, str(uuid4()))


async def test_delete_file_no_check(storage, make_bucket, make_tmp_file):
    bucket = make_bucket()
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)
    assert await storage.delete_file_no_check(bucket, tmp_file["name"])
    assert len(await storage.get_all_files(bucket)) == 0


@pytest.mark.xfail  # delete_file_no_check should return False
async def test_delete_file_not_found(storage, make_bucket, make_tmp_file):
    bucket = make_bucket()
    await storage.create_bucket(bucket)
    assert not await storage.delete_file_no_check(bucket, str(uuid4()))


async def test_soft_delete(storage, make_bucket, make_tmp_file):
    bucket = make_bucket()
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)
    await storage.soft_delete(bucket, tmp_file["name"])
    assert len(await storage.get_all_files(bucket)) == 0


@pytest.mark.xfail  # soft_delete should log "Impossible to remove file"
async def test_soft_delete_not_found(storage, make_bucket, make_tmp_file, caplog):
    bucket = make_bucket()
    await storage.create_bucket(bucket)
    await storage.soft_delete(bucket, str(uuid4()))
    assert "Impossible to remove file" in caplog.text


async def test_delete_all_files(storage, make_bucket, make_tmp_file):
    bucket = make_bucket()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, make_tmp_file())
    await upload_file(storage, bucket, make_tmp_file())
    await storage.delete_all_files_from_bucket(bucket)
    assert len(await storage.get_all_files(bucket)) == 0


async def test_generate_credentials(settings, storage, make_tmp_file):
    bucket = "test-public-exports"
    tmp_file = make_tmp_file()
    await storage.create_bucket(bucket)
    await upload_file(storage, bucket, tmp_file)

    r = await storage.generate_temporary_credentials()
    settings.AWS_ACCESS_KEY_ID = r["AccessKeyId"]
    settings.AWS_SECRET_ACCESS_KEY = r["SecretAccessKey"]
    settings.AWS_SESSION_TOKEN = r["SessionToken"]
    user_storage = Storage(settings, logging.getLogger(__name__))

    files = await user_storage.get_all_files(bucket)
    assert len(files) == 1

    file = await user_storage.get_file(bucket, tmp_file["name"])
    assert file["content"] == tmp_file["content"]
    assert file["metadata"] == tmp_file["metadata"]
    assert file["key"] == tmp_file["name"]
    assert file["size"] == len(tmp_file["content"])
