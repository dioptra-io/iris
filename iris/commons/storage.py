import asyncio
import logging
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Union

import aioboto3
import boto3
from botocore.exceptions import ClientError

from iris.commons.settings import CommonSettings


@dataclass(frozen=True)
class Storage:
    """AWS S3 object storage interface."""

    settings: CommonSettings
    logger: logging.Logger

    @property
    def aws_settings(self):
        return {
            "aws_access_key_id": self.settings.AWS_ACCESS_KEY_ID,
            "aws_secret_access_key": self.settings.AWS_SECRET_ACCESS_KEY,
            "endpoint_url": self.settings.AWS_S3_HOST,
            "region_name": self.settings.AWS_REGION_NAME,
        }

    def fault_tolerant(func):
        async def wrapper(*args, **kwargs):
            retryer = args[0].settings.storage_retryer(args[0].logger)
            return await retryer(func)(*args, **kwargs)

        return wrapper

    @fault_tolerant
    async def get_measurement_buckets(self) -> List[str]:
        """Get bucket list that is not infrastructure."""
        infrastructure_buckets = ["targets"]

        buckets = []
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            response = await s3.list_buckets()
        for bucket in response["Buckets"]:
            if bucket["Name"] in infrastructure_buckets:
                continue
            buckets.append(bucket["Name"])
        return buckets

    @fault_tolerant
    async def create_bucket(self, bucket: str) -> None:
        """Create a bucket."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            try:
                await s3.create_bucket(Bucket=bucket)
            except s3.exceptions.BucketAlreadyOwnedByYou:
                pass

    @fault_tolerant
    async def delete_bucket(self, bucket: str) -> None:
        """Delete a bucket."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            await s3.delete_bucket(Bucket=bucket)

    async def get_all_files_no_retry(self, bucket: str) -> List[Dict]:
        """Get all files inside a bucket."""
        targets = []
        session = aioboto3.Session()
        async with session.resource("s3", **self.aws_settings) as s3:
            b = await s3.Bucket(bucket)
            async for obj_summary in b.objects.all():
                try:
                    obj = await obj_summary.Object()
                    targets.append(
                        {
                            "key": obj_summary.key,
                            "size": await obj_summary.size,
                            "metadata": await obj.metadata,
                            "last_modified": str(await obj_summary.last_modified),
                        }
                    )
                except ClientError as e:
                    op = e.operation_name
                    msg = e.response.get("Error", {}).get("Message", "")
                    if op == "HeadObject" and msg == "Not Found":
                        # The file was deleted during looping. Do nothing.
                        pass
                    else:
                        raise
        return targets

    @fault_tolerant
    async def get_all_files(self, bucket: str) -> List[Dict]:
        """Get all files inside a bucket."""
        return await self.get_all_files_no_retry(bucket)

    async def get_file_no_retry(self, bucket: str, filename: str) -> Dict:
        """Get file information from a bucket."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
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
    async def get_file(self, bucket: str, filename: str) -> Dict:
        """Get file information from a bucket."""
        return await self.get_file_no_retry(bucket, filename)

    def _upload_sync_file(
        self,
        bucket: str,
        filename: str,
        filepath: Union[Path, str],
        metadata: Any = None,
    ) -> None:
        """Underlying synchronous upload function."""
        with Path(filepath).open("rb") as fd:
            s3 = boto3.client("s3", **self.aws_settings)
            extraargs = {"Metadata": metadata} if metadata else None
            s3.upload_fileobj(fd, bucket, filename, ExtraArgs=extraargs)

    @fault_tolerant
    async def upload_file(
        self,
        bucket: str,
        filename: str,
        filepath: Union[Path, str],
        metadata: Any = None,
    ) -> None:
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

    async def upload_file_no_retry(
        self, bucket: str, filename: str, fd, metadata: Any = None
    ) -> None:
        """Upload a file in a bucket with no retry."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            extraargs = {"Metadata": metadata} if metadata else None
            await s3.upload_fileobj(fd, bucket, filename, ExtraArgs=extraargs)

    def _download_sync_file(
        self, bucket: str, filename: str, output_path: Union[Path, str]
    ) -> None:
        """Underlying synchronous download function."""
        with Path(output_path).open("wb") as fd:
            s3 = boto3.client("s3", **self.aws_settings)
            s3.download_fileobj(bucket, filename, fd)

    @fault_tolerant
    async def download_file(
        self, bucket: str, filename: str, output_path: Union[Path, str]
    ) -> None:
        """Download a file in a bucket."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            ProcessPoolExecutor(),
            self._download_sync_file,
            bucket,
            filename,
            output_path,
        )

    async def delete_file_check_no_retry(self, bucket: str, filename: str) -> Dict:
        """Delete a file with a check that it exists."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                await stream.read()

            return await s3.delete_object(Bucket=bucket, Key=filename)

    @fault_tolerant
    async def delete_file_no_check(self, bucket: str, filename: str) -> bool:
        """Delete a file with no check that it exists."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            response = await s3.delete_object(Bucket=bucket, Key=filename)
        status_code: int = response["ResponseMetadata"]["HTTPStatusCode"]
        return status_code == 204

    @fault_tolerant
    async def delete_all_files_from_bucket(self, bucket: str) -> None:
        """Delete all files from a bucket."""
        session = aioboto3.Session()
        async with session.resource("s3", **self.aws_settings) as s3:
            b = await s3.Bucket(bucket)
            await b.objects.all().delete()

    @fault_tolerant
    async def copy_file_to_bucket(
        self, bucket_src: str, bucket_dest: str, filename_src: str, filename_dst: str
    ) -> None:
        """Copy a file from a bucket to another."""
        session = aioboto3.Session()
        async with session.resource("s3", **self.aws_settings) as s3:
            bucket_destination = await s3.Bucket(bucket_dest)
            await bucket_destination.copy(
                {"Bucket": bucket_src, "Key": filename_src}, filename_dst
            )
