import datetime
import json
from dataclasses import dataclass
from logging import LoggerAdapter
from pathlib import Path
from typing import Any

import aioboto3

from iris.commons.models import Round
from iris.commons.settings import CommonSettings, fault_tolerant


def next_round_key(round_: Round) -> str:
    """The name of the file containing the probes to send at the next round."""
    return f"next_round_{round_.encode()}.csv.zst"


def results_key(round_: Round) -> str:
    """The name of the file containing the results of the probing round."""
    return f"results_{round_.encode()}.csv.zst"


def targets_key(measurement_uuid: str, agent_uuid: str) -> str:
    """The name of the file containing the targets to probe."""
    return f"targets__{measurement_uuid}__{agent_uuid}.csv"


@dataclass(frozen=True)
class Storage:
    """S3 object storage interface."""

    settings: CommonSettings
    logger: LoggerAdapter

    def archive_bucket(self, user_id: str) -> str:
        return f"{self.settings.S3_PREFIX}-archive-{user_id}"

    def targets_bucket(self, user_id: str) -> str:
        return f"{self.settings.S3_PREFIX}-targets-{user_id}"

    def measurement_agent_bucket(self, measurement_uuid: str, agent_uuid: str) -> str:
        return f"{self.settings.S3_PREFIX}-{measurement_uuid[:18]}-{agent_uuid[:18]}"

    @fault_tolerant
    async def get_measurement_buckets(self) -> list[str]:
        session = aioboto3.Session()
        async with session.client("s3", **self.settings.s3) as s3:
            response = await s3.list_buckets()
            return [x["Name"] for x in response["Buckets"]]

    @fault_tolerant
    async def create_bucket(self, bucket: str) -> None:
        """Create a bucket."""
        self.logger.info("Creating bucket %s", bucket)
        session = aioboto3.Session()
        async with session.client("s3", **self.settings.s3) as s3:
            try:
                await s3.create_bucket(Bucket=bucket)
            except s3.exceptions.BucketAlreadyOwnedByYou:
                pass

    @fault_tolerant
    async def delete_bucket(self, bucket: str) -> None:
        """Delete a bucket."""
        session = aioboto3.Session()
        async with session.client("s3", **self.settings.s3) as s3:
            await s3.delete_bucket(Bucket=bucket)

    async def delete_bucket_with_files(self, bucket: str) -> None:
        await self.delete_all_files_from_bucket(bucket)
        await self.delete_bucket(bucket)

    async def get_all_files_no_retry(self, bucket: str) -> list[dict]:
        """Get all files inside a bucket."""
        targets = []
        session = aioboto3.Session()
        async with session.resource("s3", **self.settings.s3) as s3:
            b = await s3.Bucket(bucket)
            async for obj_summary in b.objects.all():
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
        return targets

    @fault_tolerant
    async def get_all_files(self, bucket: str) -> list[dict]:
        """Get all files inside a bucket."""
        return await self.get_all_files_no_retry(bucket)

    async def get_file_no_retry(
        self, bucket: str, filename: str, retrieve_content: bool = True
    ) -> dict:
        """Get file information from a bucket."""
        session = aioboto3.Session()
        async with session.client("s3", **self.settings.s3) as s3:
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

    @fault_tolerant
    async def get_file(
        self, bucket: str, filename: str, retrieve_content: bool = True
    ) -> dict:
        """Get file information from a bucket."""
        return await self.get_file_no_retry(
            bucket, filename, retrieve_content=retrieve_content
        )

    @fault_tolerant
    async def upload_file(
        self,
        bucket: str,
        filename: str,
        filepath: Path | str,
        metadata: Any = None,
    ) -> None:
        """Upload a file in a bucket."""
        with Path(filepath).open("rb") as fd:
            return await self.upload_file_no_retry(bucket, filename, fd, metadata)

    async def upload_file_no_retry(
        self, bucket: str, filename: str, fd, metadata: Any = None
    ) -> None:
        """Upload a file in a bucket with no retry."""
        session = aioboto3.Session()
        async with session.client("s3", **self.settings.s3) as s3:
            extraargs = {"Metadata": metadata} if metadata else None
            await s3.upload_fileobj(fd, bucket, filename, ExtraArgs=extraargs)

    @fault_tolerant
    async def download_file(
        self, bucket: str, filename: str, output_path: Path | str
    ) -> None:
        """Download a file in a bucket."""
        session = aioboto3.Session()
        async with session.client("s3", **self.settings.s3) as s3:
            with Path(output_path).open("wb") as fd:
                await s3.download_fileobj(bucket, filename, fd)

    async def download_file_to(self, bucket: str, filename: str, output_dir: Path):
        output_path = output_dir / filename
        await self.download_file(bucket, filename, output_path)
        return output_path

    async def delete_file_check_no_retry(self, bucket: str, filename: str) -> dict:
        """Delete a file with a check that it exists."""
        session = aioboto3.Session()
        async with session.client("s3", **self.settings.s3) as s3:
            file_object = await s3.get_object(Bucket=bucket, Key=filename)
            async with file_object["Body"] as stream:
                await stream.read()
            res: dict = await s3.delete_object(Bucket=bucket, Key=filename)
            return res

    @fault_tolerant
    async def delete_file_no_check(self, bucket: str, filename: str) -> bool:
        """Delete a file with no check that it exists."""
        session = aioboto3.Session()
        async with session.client("s3", **self.settings.s3) as s3:
            response = await s3.delete_object(Bucket=bucket, Key=filename)
        status_code: int = response["ResponseMetadata"]["HTTPStatusCode"]
        return status_code == 204

    @fault_tolerant
    async def delete_all_files_from_bucket(self, bucket: str) -> None:
        """Delete all files from a bucket."""
        session = aioboto3.Session()
        async with session.resource("s3", **self.settings.s3) as s3:
            b = await s3.Bucket(bucket)
            await b.objects.all().delete()

    @fault_tolerant
    async def copy_file_to_bucket(
        self, bucket_src: str, bucket_dest: str, filename_src: str, filename_dst: str
    ) -> None:
        """Copy a file from a bucket to another."""
        session = aioboto3.Session()
        async with session.resource("s3", **self.settings.s3) as s3:
            bucket_destination = await s3.Bucket(bucket_dest)
            await bucket_destination.copy(
                {"Bucket": bucket_src, "Key": filename_src}, filename_dst
            )

    @fault_tolerant
    async def generate_temporary_credentials(self) -> dict:
        session = aioboto3.Session()
        policy = dict(
            Version="2012-10-17",
            Statement=[
                dict(
                    Effect="Allow",
                    Action=self.settings.S3_PUBLIC_ACTIONS,
                    Resource=self.settings.S3_PUBLIC_RESOURCES,
                )
            ],
        )
        async with session.client("sts", **self.settings.s3) as sts:
            response = await sts.assume_role(
                DurationSeconds=60 * 60 * 3,
                Policy=json.dumps(policy),
                RoleArn="NotNeededForMinIO---",
                RoleSessionName="NotNeededForMinIO---",
            )
            credentials: dict = response["Credentials"]
            return credentials
