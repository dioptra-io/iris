import asyncio
import datetime
import logging
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Union
from uuid import UUID

import aioboto3
import boto3
from botocore.exceptions import ClientError

from iris.commons.schemas.public import Round
from iris.commons.settings import CommonSettings, fault_tolerant


def next_round_key(agent_uuid: UUID, round_: Round) -> str:
    """The name of the file containing the probes to send at the next round."""
    return f"{agent_uuid}_next_round_{round_.encode()}.csv.zst"


def results_key(agent_uuid: UUID, round_: Round) -> str:
    """The name of the file containing the results of the probing round."""
    return f"{agent_uuid}_results_{round_.encode()}.csv.zst"


def prefixes_key(agent_uuid: UUID, round_: Round) -> str:
    """The name of the file containing a list of allowed prefixes."""
    return f"{agent_uuid}_prefixes_{round_.encode()}.csv.zst"


def targets_key(measurement_uuid: UUID, agent_uuid: UUID) -> str:
    """The name of the file containing the targets to probe."""
    return f"targets__{measurement_uuid}__{agent_uuid}.csv"


@dataclass(frozen=True)
class Storage:
    """S3 object storage interface."""

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

    def archive_bucket(self, username: str) -> str:
        return self.settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + username

    def targets_bucket(self, username: str) -> str:
        return self.settings.AWS_S3_TARGETS_BUCKET_PREFIX + username

    @staticmethod
    def measurement_bucket(uuid: UUID) -> str:
        return str(uuid)

    @fault_tolerant(CommonSettings.storage_retry)
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

    @fault_tolerant(CommonSettings.storage_retry)
    async def create_bucket(self, bucket: str) -> None:
        """Create a bucket."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            try:
                await s3.create_bucket(Bucket=bucket)
            except s3.exceptions.BucketAlreadyOwnedByYou:
                pass

    @fault_tolerant(CommonSettings.storage_retry)
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
                            "last_modified": datetime.datetime.fromisoformat(
                                str(await obj_summary.last_modified)
                            )
                            .replace(microsecond=0)
                            .replace(tzinfo=datetime.timezone.utc),
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

    @fault_tolerant(CommonSettings.storage_retry)
    async def get_all_files(self, bucket: str) -> List[Dict]:
        """Get all files inside a bucket."""
        return await self.get_all_files_no_retry(bucket)

    async def get_file_no_retry(
        self, bucket: str, filename: str, retrieve_content: bool = True
    ) -> Dict:
        """Get file information from a bucket."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)

            content = None
            if retrieve_content:
                async with file_object["Body"] as stream:
                    content = await stream.read()
                content = content.decode("utf-8")

        return {
            "key": filename,
            "size": int(
                file_object["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            ),
            "content": content,
            "metadata": file_object["Metadata"],
            "last_modified": datetime.datetime.strptime(
                file_object["ResponseMetadata"]["HTTPHeaders"]["last-modified"],
                "%a, %d %b %Y %H:%M:%S %Z",
            ).replace(tzinfo=datetime.timezone.utc),
        }

    @fault_tolerant(CommonSettings.storage_retry)
    async def get_file(
        self, bucket: str, filename: str, retrieve_content: bool = True
    ) -> Dict:
        """Get file information from a bucket."""
        return await self.get_file_no_retry(
            bucket, filename, retrieve_content=retrieve_content
        )

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

    @fault_tolerant(CommonSettings.storage_retry)
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

    @fault_tolerant(CommonSettings.storage_retry)
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

    async def download_file_to(self, bucket: str, filename: str, output_dir: Path):
        output_path = output_dir / filename
        await self.download_file(bucket, filename, output_path)
        return output_path

    async def delete_file_check_no_retry(self, bucket: str, filename: str) -> Dict:
        """Delete a file with a check that it exists."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                await stream.read()
            res: Dict = await s3.delete_object(Bucket=bucket, Key=filename)
            return res

    @fault_tolerant(CommonSettings.storage_retry)
    async def delete_file_no_check(self, bucket: str, filename: str) -> bool:
        """Delete a file with no check that it exists."""
        session = aioboto3.Session()
        async with session.client("s3", **self.aws_settings) as s3:
            response = await s3.delete_object(Bucket=bucket, Key=filename)
        status_code: int = response["ResponseMetadata"]["HTTPStatusCode"]
        return status_code == 204

    @fault_tolerant(CommonSettings.storage_retry)
    async def delete_all_files_from_bucket(self, bucket: str) -> None:
        """Delete all files from a bucket."""
        session = aioboto3.Session()
        async with session.resource("s3", **self.aws_settings) as s3:
            b = await s3.Bucket(bucket)
            await b.objects.all().delete()

    async def soft_delete(self, bucket: str, filename: str) -> None:
        is_deleted = await self.delete_file_no_check(bucket, filename)
        if not is_deleted:
            self.logger.error(f"Impossible to remove file `{filename}` from S3")

    @fault_tolerant(CommonSettings.storage_retry)
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
