import logging
import uuid
from datetime import datetime

import pytest

from iris.commons.database import Measurements


@pytest.mark.asyncio
async def test_measurements(common_settings):
    db = Measurements(common_settings, logging.getLogger(__name__))
    assert await db.create_database() is None
    assert await db.create_table(drop=True) is None

    data = [
        {
            "measurement_uuid": uuid.uuid4(),
            "user": "foo",
            "tool": "bar",
            "tags": ["tag1"],
            "start_time": int(datetime.now().timestamp()),
        },
        {
            "measurement_uuid": uuid.uuid4(),
            "user": "foo",
            "tool": "bar",
            "tags": ["tag1", "tag2"],
            "start_time": int(datetime.now().timestamp()),
        },
    ]

    for obj in data:
        assert await db.register(obj) is None

    assert await db.all_count(user="foo") == 2
    assert await db.all_count(user="foo", tag="tag1") == 2
    assert await db.all_count(user="foo", tag="tag2") == 1

    for obj in data:
        assert await db.get(user="foo", uuid=obj["measurement_uuid"]) == {
            "uuid": str(obj["measurement_uuid"]),
            "user": obj["user"],
            "tool": obj["tool"],
            "tags": obj["tags"],
            "state": "ongoing",
            "start_time": datetime.fromtimestamp(obj["start_time"]).isoformat(),
            "end_time": None,
        }

    assert await db.all(user="foo", tag="unknown", offset=0, limit=10) == []
    assert await db.get(user="foo", uuid=uuid.uuid4()) is None

    all1 = await db.all(user="foo", offset=0, limit=10)
    all2 = [await db.get(user="foo", uuid=obj["measurement_uuid"]) for obj in data]
    assert sorted(all1, key=lambda x: x["uuid"]) == sorted(
        all2, key=lambda x: x["uuid"]
    )

    assert await db.stamp_canceled(user="foo", uuid=data[0]["measurement_uuid"]) is None
    res = await db.get(user="foo", uuid=data[0]["measurement_uuid"])
    assert res["state"] == "canceled"
    assert res["end_time"] is None

    assert await db.stamp_finished(user="foo", uuid=data[1]["measurement_uuid"]) is None
    assert await db.stamp_end_time(user="foo", uuid=data[1]["measurement_uuid"]) is None
    res = await db.get(user="foo", uuid=data[1]["measurement_uuid"])
    assert res["state"] == "finished"
    assert res["end_time"] is not None
