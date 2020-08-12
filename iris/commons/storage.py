import aioboto3
import asyncio
import boto3

from aiohttp.client_exceptions import ClientConnectorError, ServerTimeoutError
from iris.commons.settings import CommonSettings

common_settings = CommonSettings()


def retry_on_failure(func):
    async def wrapper(*args, **kwargs):
        for _ in range(common_settings.AWS_TIMEOUT_RETRIES):
            try:
                return await func(*args, **kwargs)
            except (ServerTimeoutError, ClientConnectorError):
                await asyncio.sleep(common_settings.AWS_TIMEOUT_WAIT)
        raise Exception("AWS TimeOut error")

    return wrapper


class Storage(object):
    """AWS S3 object storage interface."""

    def __init__(self, settings=None):
        self.build_settings(settings)

    def build_settings(self, settings=None):
        if not settings:
            settings = common_settings

        self.settings = {
            "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
            "endpoint_url": settings.AWS_S3_HOST,
            "region_name": settings.AWS_REGION_NAME,
        }

    @retry_on_failure
    async def get_measurement_buckets(self):
        """Get bucket list that is not infrastructure."""
        infrastructure_buckets = ["targets"]

        buckets = []
        async with aioboto3.client("s3", **self.settings) as s3:
            response = await s3.list_buckets()
        for bucket in response["Buckets"]:
            if bucket["Name"] in infrastructure_buckets:
                continue
            buckets.append(bucket["Name"])
        return buckets

    @retry_on_failure
    async def create_bucket(self, bucket):
        """Create a bucket."""
        async with aioboto3.client("s3", **self.settings) as s3:
            await s3.create_bucket(Bucket=bucket)

    @retry_on_failure
    async def delete_bucket(self, bucket):
        """Delete a bucket."""
        async with aioboto3.client("s3", **self.settings) as s3:
            await s3.delete_bucket(Bucket=bucket)

    @retry_on_failure
    async def get_all_files(self, bucket):
        """Get all files inside a bucket."""
        targets = []
        async with aioboto3.resource("s3", **self.settings) as s3:
            bucket = await s3.Bucket(bucket)
            async for file_object in bucket.objects.all():
                file_size = await file_object.size
                last_modified = str(await file_object.last_modified)
                targets.append(
                    {
                        "key": file_object.key,
                        "size": file_size,
                        "last_modified": last_modified,
                    }
                )
        return targets

    @retry_on_failure
    async def get_file(self, bucket, filename):
        """Get file information from a bucket."""
        async with aioboto3.client("s3", **self.settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                await stream.read()

        return {
            "key": filename,
            "size": int(
                file_object["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            ),
            "last_modified": file_object["ResponseMetadata"]["HTTPHeaders"][
                "last-modified"
            ],
        }

    def _upload_sync_file(self, bucket, filename, fin):
        """Underlying synchronous upload function."""
        s3 = boto3.client("s3", **self.settings)
        s3.upload_fileobj(fin, bucket, filename)

    @retry_on_failure
    async def upload_file(self, bucket, filename, fin):
        """Upload a file in a bucket."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._upload_sync_file, bucket, filename, fin)

    @retry_on_failure
    async def download_file(self, bucket, filename, output_path):
        """Download a file from a bucket."""
        async with aioboto3.client("s3", **self.settings) as s3:
            await s3.download_file(bucket, filename, output_path)

    @retry_on_failure
    async def delete_file_check(self, bucket, filename):
        """Delete a file with a check that it exists."""
        async with aioboto3.client("s3", **self.settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                await stream.read()

            return await s3.delete_object(Bucket=bucket, Key=filename)

    @retry_on_failure
    async def delete_file_no_check(self, bucket, filename):
        """Delete a file with no check that it exists."""
        async with aioboto3.client("s3", **self.settings) as s3:
            return await s3.delete_object(Bucket=bucket, Key=filename)

    @retry_on_failure
    async def delete_all_files_from_bucket(self, bucket):
        """Delete all files from a bucket."""
        async with aioboto3.resource("s3", **self.settings) as s3:
            bucket = await s3.Bucket(bucket)
            await bucket.objects.all().delete()
