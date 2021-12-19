import uuid

import pytest

from iris.commons.database import measurements
from iris.commons.schemas.private import MeasurementRequest
from iris.commons.schemas.public import Measurement, MeasurementState


@pytest.mark.asyncio
async def test_measurements(database):

    user_id = uuid.uuid4()

    assert await measurements.create_table(database, drop=True) is None

    data = [
        MeasurementRequest(agents=[], tags=["tag1"], user_id=user_id),
        MeasurementRequest(agents=[], tags=["tag1", "tag2"], user_id=user_id),
    ]

    for obj in data:
        assert await measurements.register(database, obj) is None

    assert await measurements.all_count(database, user_id=user_id) == 2
    assert await measurements.all_count(database, user_id=user_id, tag="tag1") == 2
    assert await measurements.all_count(database, user_id=user_id, tag="tag2") == 1

    for obj in data:
        assert await measurements.get(
            database, user_id=user_id, uuid=obj.uuid
        ) == Measurement(
            uuid=obj.uuid,
            user_id=obj.user_id,
            tool=obj.tool,
            tags=obj.tags,
            start_time=obj.start_time,
            state=MeasurementState.Ongoing,
            agents=[],
        )

    assert (
        await measurements.all(
            database, user_id=user_id, tag="unknown", offset=0, limit=10
        )
        == []
    )
    assert await measurements.get(database, user_id=user_id, uuid=uuid.uuid4()) is None

    all1 = await measurements.all(database, user_id=user_id, offset=0, limit=10)
    all2 = [
        await measurements.get(database, user_id=user_id, uuid=obj.uuid) for obj in data
    ]
    assert sorted(all1, key=lambda x: x.uuid) == sorted(all2, key=lambda x: x.uuid)

    assert (
        await measurements.set_state(
            database,
            user_id=user_id,
            uuid=data[0].uuid,
            state=MeasurementState.Canceled,
        )
        is None
    )
    res = await measurements.get(database, user_id=user_id, uuid=data[0].uuid)
    assert res.state == MeasurementState.Canceled
    assert res.end_time is None

    assert (
        await measurements.set_state(
            database,
            user_id=user_id,
            uuid=data[1].uuid,
            state=MeasurementState.Finished,
        )
        is None
    )
    assert (
        await measurements.set_end_time(database, user_id=user_id, uuid=data[1].uuid)
        is None
    )
    res = await measurements.get(database, user_id=user_id, uuid=data[1].uuid)
    assert res.state == MeasurementState.Finished
    assert res.end_time is not None
