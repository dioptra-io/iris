import json
import shutil
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LocalStorage:
    s3_dir: Path

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
