import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from iris.commons.settings import CommonSettings


@dataclass(frozen=True)
class LocalStorage:
    settings: CommonSettings
    s3_dir: Path

    def archive_bucket(self, username: str) -> str:
        return self.settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + username

    def targets_bucket(self, username: str) -> str:
        return self.settings.AWS_S3_TARGETS_BUCKET_PREFIX + username

    @staticmethod
    def measurement_bucket(uuid: UUID) -> str:
        return str(uuid)

    async def delete_file_no_check(self, bucket, filename):
        self.__file_path(bucket, filename).unlink(missing_ok=True)
        return {"ResponseMetadata": {"HTTPStatusCode": 204}}

    async def download_file(self, bucket, filename, output_path):
        input_path = self.__file_path(bucket, filename)
        self.__ensure_dir(output_path)
        shutil.copyfile(input_path, output_path)

    async def upload_file(self, bucket, filename, input_path, metadata=None):
        output_path = self.__file_path(bucket, filename)
        metadata_path = self.__meta_path(bucket, filename)
        self.__ensure_dir(output_path)
        shutil.copyfile(input_path, output_path)
        if metadata:
            metadata_path.write_text(json.dumps(metadata))

    async def get_file(self, bucket, filename):
        metadata_path = self.__meta_path(bucket, filename)
        metadata = None
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text())
        return {"key": filename, "metadata": metadata}

    @staticmethod
    def __ensure_dir(output_path):
        Path(output_path).parent.mkdir(exist_ok=True, parents=True)

    def __bucket_path(self, bucket):
        return self.s3_dir / bucket

    def __file_path(self, bucket, filename):
        return self.__bucket_path(bucket) / filename

    def __meta_path(self, bucket, filename):
        return self.__file_path(bucket, filename).with_suffix(".metadata")
