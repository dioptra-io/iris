import os
import tempfile

import pytest

superuser = pytest.mark.skipif(
    os.geteuid() != 0, reason="this test must be run as root"
)


class TestUploadFile:
    def __init__(self, content):
        self.file = tempfile.SpooledTemporaryFile()
        if isinstance(content, str):
            content = content.encode()
        self.file.write(content)
        self.file.seek(0)


def add_and_refresh(session, instances):
    session.add_all(instances)
    session.commit()
    for instance in instances:
        session.refresh(instance)


async def upload_file(storage, bucket, tmp_file):
    await storage.upload_file(
        bucket, tmp_file["name"], tmp_file["path"], tmp_file["metadata"]
    )
