import os
import tempfile
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List
from uuid import uuid4

import pytest

from iris.commons.models import AgentParameters, AgentState, User
from iris.commons.redis import Redis
from iris.commons.storage import Storage, targets_key

skipci = pytest.mark.skipif(
    "CI" in os.environ, reason="this test is not supported on GitHub actions"
)

superuser = pytest.mark.skipif(
    os.geteuid() != 0, reason="this test must be run as root"
)


class FakeUploadFile:
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


async def create_user_buckets(storage: Storage, user: User):
    await storage.create_bucket(storage.archive_bucket(str(user.id)))
    await storage.create_bucket(storage.targets_bucket(str(user.id)))


async def archive_target_file(
    storage: Storage, user: User, measurement_uuid: str, agent_uuid: str, filename: str
):
    await storage.copy_file_to_bucket(
        storage.targets_bucket(str(user.id)),
        storage.archive_bucket(str(user.id)),
        filename,
        targets_key(measurement_uuid, agent_uuid),
    )


async def upload_target_file(
    storage: Storage, user: User, filename: str, content: List[str]
):
    with TemporaryDirectory() as directory:
        file = Path(directory) / filename
        file.write_text("\n".join(content))
        await storage.upload_file(
            storage.targets_bucket(str(user.id)), filename, str(file)
        )


async def register_agent(
    redis: Redis, uuid: str, parameters: AgentParameters, state: AgentState
):
    await redis.register_agent(uuid, 5)
    await redis.set_agent_parameters(uuid, parameters)
    await redis.set_agent_state(uuid, state)


async def register_user(client, **kwargs):
    default = dict(
        email=f"{uuid4()}@example.org",
        password="password",
        firstname="firstname",
        lastname="lastname",
    )
    return client.post("/auth/register", json={**default, **kwargs})
