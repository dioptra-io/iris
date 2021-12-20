import uuid
from datetime import datetime

import pytest

from iris.api.dependencies import get_redis, get_storage
from iris.commons.database import agents, measurements
from iris.commons.schemas.measurements import (
    FlowMapper,
    Measurement,
    MeasurementAgent,
    MeasurementAgentSpecific,
    MeasurementState,
    MeasurementSummary,
    Tool,
    ToolParameters,
)
from iris.commons.schemas.paging import Paginated
from tests.helpers import async_mock, fake_redis_factory, fake_storage_factory, override

target23 = {
    "key": "test.csv",
    "size": 42,
    "content": "8.8.8.0/23,icmp,2,32,6",
    "last_modified": datetime(2021, 9, 20, 13, 20, 26),
    "metadata": None,
}

target25 = {
    "key": "test.csv",
    "size": 42,
    "content": "8.8.8.0/25,icmp,2,32,6",
    "last_modified": datetime(2021, 9, 20, 13, 20, 26),
    "metadata": None,
}

target_probes = {
    "key": "probes.csv",
    "size": 42,
    "content": "8.8.8.8,24000,33434,32,icmp",
    "last_modified": datetime(2021, 9, 20, 13, 20, 26),
    "metadata": {"is_probes_file": "True"},
}


@pytest.fixture(scope="function")
def measurement1():
    return Measurement(
        uuid=uuid.uuid4(),
        user_id=uuid.uuid4(),
        state=MeasurementState.Unknown,
        tool=Tool.DiamondMiner,
        agents=[],
        tags=["test"],
        start_time=datetime.utcnow(),
        end_time=datetime.utcnow(),
    )


@pytest.fixture(scope="function")
def measurement_agent1(agent, statistics):
    return MeasurementAgent(
        uuid=agent.uuid,
        state=MeasurementState.Unknown,
        specific=MeasurementAgentSpecific(
            target_file="test.csv",
            target_file_content=["8.8.8.0/23,icmp,2,32,6"],
            probing_rate=None,
            tool_parameters=ToolParameters(
                initial_source_port=24000,
                destination_port=33434,
                flow_mapper=FlowMapper.IntervalFlowMapper,
                flow_mapper_kwargs={},
                max_round=5,
                global_min_ttl=0,
                global_max_ttl=255,
            ),
        ),
        parameters=agent.parameters,
        probing_statistics=[statistics],
    )


# --- GET /measurements ---


def test_get_measurements_empty(api_client_sync, monkeypatch):
    monkeypatch.setattr(measurements, "all", async_mock([]))
    monkeypatch.setattr(measurements, "all_count", async_mock(0))
    response = api_client_sync.get("/measurements/public/")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=0, results=[]
    )


def test_get_measurements(api_client_sync, monkeypatch):
    measurements_ = [
        Measurement(
            uuid=uuid.uuid4(),
            user_id=uuid.uuid4(),
            state=MeasurementState.Finished,
            tool=Tool.DiamondMiner,
            agents=[],
            tags=[],
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow(),
        ),
        Measurement(
            uuid=uuid.uuid4(),
            user_id=uuid.uuid4(),
            state=MeasurementState.Finished,
            tool=Tool.DiamondMiner,
            agents=[],
            tags=[],
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow(),
        ),
        Measurement(
            uuid=uuid.uuid4(),
            user_id=uuid.uuid4(),
            state=MeasurementState.Finished,
            tool=Tool.DiamondMiner,
            agents=[],
            tags=[],
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow(),
        ),
    ]

    measurements_ = sorted(measurements_, key=lambda x: x.start_time, reverse=True)
    summaries = [
        MeasurementSummary(**x.dict(exclude={"agents", "user_id"}))
        for x in measurements_
    ]

    async def all(self, offset, limit, user_id=None, tag=None):
        return measurements_[offset : offset + limit]  # noqa : E203

    override(
        api_client_sync,
        get_redis,
        fake_redis_factory(measurement_state=MeasurementState.Finished),
    )
    monkeypatch.setattr(measurements, "all", all)
    monkeypatch.setattr(measurements, "all_count", async_mock(3))

    # No (offset, limit)
    response = api_client_sync.get("/measurements/public/")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3, results=summaries
    )

    # All inclusive (0, 100)
    response = api_client_sync.get("/measurements/public/?offset=0&limit=100")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3, results=summaries
    )

    # First result (0, 1)
    response = api_client_sync.get("/measurements/public/?offset=0&limit=1")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3,
        next="http://testserver/measurements/public/?limit=1&offset=1",
        results=summaries[:1],
    )

    # Middle result (1, 1)
    response = api_client_sync.get("/measurements/public/?offset=1&limit=1")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3,
        next="http://testserver/measurements/public/?limit=1&offset=2",
        previous="http://testserver/measurements/public/?limit=1",
        results=summaries[1:2],
    )

    # Last result (2, 1)
    response = api_client_sync.get("/measurements/public/?offset=2&limit=1")
    assert Paginated[MeasurementSummary](**response.json()) == Paginated(
        count=3,
        previous="http://testserver/measurements/public/?limit=1&offset=1",
        results=summaries[2:3],
    )


# --- GET /measurements/{measurement_uuid} ---


def test_get_measurement_by_uuid(
    api_client_sync, measurement1, measurement_agent1, monkeypatch
):
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr(agents, "all", async_mock([measurement_agent1]))
    monkeypatch.setattr(measurements, "get", async_mock(measurement1))
    expected = measurement1.copy(update={"agents": [measurement_agent1]})
    response = api_client_sync.get(f"/measurements/{measurement1.uuid}")
    assert Measurement(**response.json()) == expected


def test_get_measurement_by_uuid_waiting(
    api_client_sync, measurement1, measurement_agent1, monkeypatch
):
    override(
        api_client_sync,
        get_redis,
        fake_redis_factory(measurement_state=MeasurementState.Waiting),
    )
    override(api_client_sync, get_storage, fake_storage_factory([target23]))
    monkeypatch.setattr(agents, "all", async_mock([measurement_agent1]))
    monkeypatch.setattr(measurements, "get", async_mock(measurement1))
    expected = measurement1.copy(
        update={
            "state": MeasurementState.Waiting,
            "agents": [
                measurement_agent1.copy(update={"state": MeasurementState.Waiting})
            ],
        }
    )
    response = api_client_sync.get(f"/measurements/public/{measurement1.uuid}")
    assert Measurement(**response.json()).dict() == expected.dict()


def test_get_measurement_by_uuid_not_found(api_client_sync, monkeypatch):
    monkeypatch.setattr(measurements, "get", async_mock(None))
    measurement_uuid = str(uuid.uuid4())
    response = api_client_sync.get(f"/measurements/{measurement_uuid}")
    assert response.status_code == 404
    assert response.json() == {"detail": "Measurement not found"}


def test_get_measurement_by_uuid_invalid_input(api_client_sync):
    response = api_client_sync.get("/measurements/public/test")
    assert response.status_code == 422
