import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import aioboto3
import boto3
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_delay,
    wait_exponential,
    wait_random,
)


class Storage(object):
    """AWS S3 object storage interface."""

    def __init__(self, settings, logger=None):
        self.logger = logger
        self.settings = settings
        self.aws_settings = {
            "aws_access_key_id": settings.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
            "endpoint_url": settings.AWS_S3_HOST,
            "region_name": settings.AWS_REGION_NAME,
        }

    def fault_tolerant(func):
        """Exponential back-off strategy."""

        async def wrapper(*args, **kwargs):
            cls = args[0]
            settings, logger = cls.settings, cls.logger
            return await retry(
                stop=stop_after_delay(settings.AWS_TIMEOUT),
                wait=wait_exponential(
                    multiplier=settings.AWS_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
                    min=settings.AWS_TIMEOUT_EXPONENTIAL_MIN,
                    max=settings.AWS_TIMEOUT_EXPONENTIAL_MAX,
                )
                + wait_random(
                    settings.AWS_TIMEOUT_RANDOM_MIN,
                    settings.AWS_TIMEOUT_RANDOM_MAX,
                ),
                before_sleep=(
                    before_sleep_log(logger, logging.ERROR) if logger else None
                ),
            )(func)(*args, **kwargs)

        return wrapper

    @fault_tolerant
    async def get_measurement_buckets(self):
        """Get bucket list that is not infrastructure."""
        infrastructure_buckets = ["targets"]

        buckets = []
        async with aioboto3.client("s3", **self.aws_settings) as s3:
            response = await s3.list_buckets()
        for bucket in response["Buckets"]:
            if bucket["Name"] in infrastructure_buckets:
                continue
            buckets.append(bucket["Name"])
        return buckets

    @fault_tolerant
    async def create_bucket(self, bucket):
        """Create a bucket."""
        async with aioboto3.client("s3", **self.aws_settings) as s3:
            try:
                await s3.create_bucket(Bucket=bucket)
            except s3.exceptions.BucketAlreadyOwnedByYou:
                pass

    @fault_tolerant
    async def delete_bucket(self, bucket):
        """Delete a bucket."""
        async with aioboto3.client("s3", **self.aws_settings) as s3:
            await s3.delete_bucket(Bucket=bucket)

    async def get_all_files_no_retry(self, bucket):
        """Get all files inside a bucket."""
        targets = []
        async with aioboto3.resource("s3", **self.aws_settings) as s3:
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

    @fault_tolerant
    async def get_all_files(self, bucket):
        """Get all files inside a bucket."""
        return await self.get_all_files_no_retry(bucket)

    async def get_file_no_retry(self, bucket, filename):
        """Get file information from a bucket."""
        async with aioboto3.client("s3", **self.aws_settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                content = await stream.read()

        return {
            "key": filename,
            "size": int(
                file_object["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            ),
            "content": content.decode("utf-8"),
            "metadata": file_object["Metadata"],
            "last_modified": file_object["ResponseMetadata"]["HTTPHeaders"][
                "last-modified"
            ],
        }

    @fault_tolerant
    async def get_file(self, bucket, filename):
        """Get file information from a bucket."""
        return await self.get_file_no_retry(bucket, filename)

    def _upload_sync_file(self, bucket, filename, filepath, metadata=None):
        """Underlying synchronous upload function."""
        with Path(filepath).open("rb") as fd:
            s3 = boto3.client("s3", **self.aws_settings)
            extraargs = {"Metadata": metadata} if metadata else None
            s3.upload_fileobj(fd, bucket, filename, ExtraArgs=extraargs)

    @fault_tolerant
    async def upload_file(self, bucket, filename, filepath, metadata=None):
        """Upload a file in a bucket."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            ProcessPoolExecutor(),
            self._upload_sync_file,
            bucket,
            filename,
            filepath,
            metadata,
        )

    async def upload_file_no_retry(self, bucket, filename, fd, metadata=None):
        """Upload a file in a bucket with no retry."""
        async with aioboto3.client("s3", **self.aws_settings) as s3:
            extraargs = {"Metadata": metadata} if metadata else None
            await s3.upload_fileobj(fd, bucket, filename, ExtraArgs=extraargs)

    def _download_sync_file(self, bucket, filename, output_path):
        """Underlying synchronous download function."""
        with Path(output_path).open("wb") as fd:
            s3 = boto3.client("s3", **self.aws_settings)
            s3.download_fileobj(bucket, filename, fd)

    @fault_tolerant
    async def download_file(self, bucket, filename, output_path):
        """Download a file in a bucket."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            ProcessPoolExecutor(),
            self._download_sync_file,
            bucket,
            filename,
            output_path,
        )

    async def delete_file_check_no_retry(self, bucket, filename):
        """Delete a file with a check that it exists."""
        async with aioboto3.client("s3", **self.aws_settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                await stream.read()

            return await s3.delete_object(Bucket=bucket, Key=filename)

    @fault_tolerant
    async def delete_file_no_check(self, bucket, filename):
        """Delete a file with no check that it exists."""
        async with aioboto3.client("s3", **self.aws_settings) as s3:
            response = await s3.delete_object(Bucket=bucket, Key=filename)
        return response["ResponseMetadata"]["HTTPStatusCode"] == 204

    @fault_tolerant
    async def delete_all_files_from_bucket(self, bucket):
        """Delete all files from a bucket."""
        async with aioboto3.resource("s3", **self.aws_settings) as s3:
            bucket = await s3.Bucket(bucket)
            await bucket.objects.all().delete()

    @fault_tolerant
    async def copy_file_to_bucket(
        self, bucket_src, bucket_dest, filename_src, filename_dst
    ):
        """Copy a file from a bucket to another."""
        async with aioboto3.resource("s3", **self.aws_settings) as s3:
            bucket_destination = await s3.Bucket(bucket_dest)
            await bucket_destination.copy(
                {"Bucket": bucket_src, "Key": filename_src}, filename_dst
            )
