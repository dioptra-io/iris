import uuid

import pytest

from iris.commons.database import measurements
from iris.commons.schemas.private import MeasurementRequest
from iris.commons.schemas.public import Measurement, MeasurementState


@pytest.mark.asyncio
async def test_measurements(database):
    assert await measurements.create_table(database, drop=True) is None

    data = [
        MeasurementRequest(agents=[], tags=["tag1"], username="foo"),
        MeasurementRequest(agents=[], tags=["tag1", "tag2"], username="foo"),
    ]

    for obj in data:
        assert await measurements.register(database, obj) is None

    assert await measurements.all_count(database, user="foo") == 2
    assert await measurements.all_count(database, user="foo", tag="tag1") == 2
    assert await measurements.all_count(database, user="foo", tag="tag2") == 1

    for obj in data:
        assert await measurements.get(
            database, user="foo", uuid=obj.uuid
        ) == Measurement(
            uuid=obj.uuid,
            username=obj.username,
            tool=obj.tool,
            tags=obj.tags,
            start_time=obj.start_time,
            state=MeasurementState.Ongoing,
            agents=[],
        )

    assert (
        await measurements.all(database, user="foo", tag="unknown", offset=0, limit=10)
        == []
    )
    assert await measurements.get(database, user="foo", uuid=uuid.uuid4()) is None

    all1 = await measurements.all(database, user="foo", offset=0, limit=10)
    all2 = [await measurements.get(database, user="foo", uuid=obj.uuid) for obj in data]
    assert sorted(all1, key=lambda x: x.uuid) == sorted(all2, key=lambda x: x.uuid)

    assert (
        await measurements.stamp_canceled(database, user="foo", uuid=data[0].uuid)
        is None
    )
    res = await measurements.get(database, user="foo", uuid=data[0].uuid)
    assert res.state == MeasurementState.Canceled
    assert res.end_time is None

    assert (
        await measurements.stamp_finished(database, user="foo", uuid=data[1].uuid)
        is None
    )
    assert (
        await measurements.stamp_end_time(database, user="foo", uuid=data[1].uuid)
        is None
    )
    res = await measurements.get(database, user="foo", uuid=data[1].uuid)
    assert res.state == MeasurementState.Finished
    assert res.end_time is not None
