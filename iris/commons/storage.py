import aioboto3
import asyncio
import boto3
import logging

from iris.commons import logger
from iris.commons.settings import CommonSettings
from tenacity import (
    retry,
    stop_after_delay,
    wait_exponential,
    wait_random,
    before_sleep_log,
)
from pathlib import Path

common_settings = CommonSettings()


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

    @retry(
        stop=stop_after_delay(common_settings.AWS_TIMEOUT),
        wait=wait_exponential(
            multiplier=common_settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
            min=common_settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
            max=common_settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
        )
        + wait_random(
            common_settings.AWS_TIMEOUT_RANDOM_MIN,
            common_settings.AWS_TIMEOUT_RANDOM_MAX,
        ),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
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

    @retry(
        stop=stop_after_delay(common_settings.AWS_TIMEOUT),
        wait=wait_exponential(
            multiplier=common_settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
            min=common_settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
            max=common_settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
        )
        + wait_random(
            common_settings.AWS_TIMEOUT_RANDOM_MIN,
            common_settings.AWS_TIMEOUT_RANDOM_MAX,
        ),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
    async def create_bucket(self, bucket):
        """Create a bucket."""
        async with aioboto3.client("s3", **self.settings) as s3:
            try:
                await s3.create_bucket(Bucket=bucket)
            except s3.exceptions.BucketAlreadyOwnedByYou:
                pass

    @retry(
        stop=stop_after_delay(common_settings.AWS_TIMEOUT),
        wait=wait_exponential(
            multiplier=common_settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
            min=common_settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
            max=common_settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
        )
        + wait_random(
            common_settings.AWS_TIMEOUT_RANDOM_MIN,
            common_settings.AWS_TIMEOUT_RANDOM_MAX,
        ),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
    async def delete_bucket(self, bucket):
        """Delete a bucket."""
        async with aioboto3.client("s3", **self.settings) as s3:
            await s3.delete_bucket(Bucket=bucket)

    @retry(
        stop=stop_after_delay(common_settings.AWS_TIMEOUT),
        wait=wait_exponential(
            multiplier=common_settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
            min=common_settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
            max=common_settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
        )
        + wait_random(
            common_settings.AWS_TIMEOUT_RANDOM_MIN,
            common_settings.AWS_TIMEOUT_RANDOM_MAX,
        ),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
    async def get_all_files(self, bucket):
        """Get all files inside a bucket."""
        targets = []
        async with aioboto3.resource("s3", **self.settings) as s3:
            bucket = await s3.Bucket(bucket)
            async for obj_summary in bucket.objects.all():
                obj = await obj_summary.Object()
                targets.append(
                    {
                        "key": obj_summary.key,
                        "size": await obj_summary.size,
                        "metadata": await obj.metadata,
                        "last_modified": str(await obj_summary.last_modified),
                    }
                )
        return targets

    async def get_all_files_no_retry(self, bucket):
        """Get all files inside a bucket."""
        targets = []
        async with aioboto3.resource("s3", **self.settings) as s3:
            bucket = await s3.Bucket(bucket)
            async for obj_summary in bucket.objects.all():
                obj = await obj_summary.Object()
                targets.append(
                    {
                        "key": obj_summary.key,
                        "size": await obj_summary.size,
                        "metadata": await obj.metadata,
                        "last_modified": str(await obj_summary.last_modified),
                    }
                )
        return targets

    @retry(
        stop=stop_after_delay(common_settings.AWS_TIMEOUT),
        wait=wait_exponential(
            multiplier=common_settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
            min=common_settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
            max=common_settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
        )
        + wait_random(
            common_settings.AWS_TIMEOUT_RANDOM_MIN,
            common_settings.AWS_TIMEOUT_RANDOM_MAX,
        ),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
    async def get_file(self, bucket, filename):
        """Get file information from a bucket."""
        async with aioboto3.client("s3", **self.settings) as s3:
            try:
                file_object = await s3.get_object(Bucket=bucket, Key=filename)
            except s3.exceptions.NoSuchKey:
                return None
            async with file_object["Body"] as stream:
                await stream.read()
        return {
            "key": filename,
            "size": int(
                file_object["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            ),
            "metadata": file_object["Metadata"],
            "last_modified": file_object["ResponseMetadata"]["HTTPHeaders"][
                "last-modified"
            ],
        }

    async def get_file_no_retry(self, bucket, filename):
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
            "metadata": file_object["Metadata"],
            "last_modified": file_object["ResponseMetadata"]["HTTPHeaders"][
                "last-modified"
            ],
        }

    def _upload_sync_file(self, bucket, filename, fin, metadata=None):
        """Underlying synchronous upload function."""
        s3 = boto3.client("s3", **self.settings)
        extraargs = {"Metadata": metadata} if metadata else None
        s3.upload_fileobj(fin, bucket, filename, ExtraArgs=extraargs)

    @retry(
        stop=stop_after_delay(common_settings.AWS_TIMEOUT),
        wait=wait_exponential(
            multiplier=common_settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
            min=common_settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
            max=common_settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
        )
        + wait_random(
            common_settings.AWS_TIMEOUT_RANDOM_MIN,
            common_settings.AWS_TIMEOUT_RANDOM_MAX,
        ),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
    async def upload_file(self, bucket, filename, filepath, metadata=None):
        """Upload a file in a bucket."""
        with Path(filepath).open("rb") as fd:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, self._upload_sync_file, bucket, filename, fd, metadata
            )

    async def upload_file_no_retry(self, bucket, filename, fd, metadata=None):
        """Upload a file in a bucket with no retry."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, self._upload_sync_file, bucket, filename, fd, metadata
        )

    def _download_sync_file(self, bucket, filename, fd):
        """Underlying synchronous download function."""
        s3 = boto3.client("s3", **self.settings)
        s3.download_fileobj(bucket, filename, fd)

    @retry(
        stop=stop_after_delay(common_settings.AWS_TIMEOUT),
        wait=wait_exponential(
            multiplier=common_settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
            min=common_settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
            max=common_settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
        )
        + wait_random(
            common_settings.AWS_TIMEOUT_RANDOM_MIN,
            common_settings.AWS_TIMEOUT_RANDOM_MAX,
        ),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
    async def download_file(self, bucket, filename, output_path):
        """Download a file in a bucket."""
        loop = asyncio.get_running_loop()
        with Path(output_path).open("wb") as fd:
            await loop.run_in_executor(
                None, self._download_sync_file, bucket, filename, fd
            )

    async def delete_file_check_no_retry(self, bucket, filename):
        """Delete a file with a check that it exists."""
        async with aioboto3.client("s3", **self.settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                await stream.read()

            return await s3.delete_object(Bucket=bucket, Key=filename)

    @retry(
        stop=stop_after_delay(common_settings.AWS_TIMEOUT),
        wait=wait_exponential(
            multiplier=common_settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
            min=common_settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
            max=common_settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
        )
        + wait_random(
            common_settings.AWS_TIMEOUT_RANDOM_MIN,
            common_settings.AWS_TIMEOUT_RANDOM_MAX,
        ),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
    async def delete_file_no_check(self, bucket, filename):
        """Delete a file with no check that it exists."""
        async with aioboto3.client("s3", **self.settings) as s3:
            return await s3.delete_object(Bucket=bucket, Key=filename)

    @retry(
        stop=stop_after_delay(common_settings.AWS_TIMEOUT),
        wait=wait_exponential(
            multiplier=common_settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
            min=common_settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
            max=common_settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
        )
        + wait_random(
            common_settings.AWS_TIMEOUT_RANDOM_MIN,
            common_settings.AWS_TIMEOUT_RANDOM_MAX,
        ),
        before_sleep=before_sleep_log(logger, logging.ERROR),
    )
    async def delete_all_files_from_bucket(self, bucket):
        """Delete all files from a bucket."""
        async with aioboto3.resource("s3", **self.settings) as s3:
            bucket = await s3.Bucket(bucket)
            await bucket.objects.all().delete()
