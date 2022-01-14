from datetime import datetime

from iris.commons.models import MeasurementAgentState


def test_start_end_time_no_agents(make_measurement, make_measurement_agent):
    measurement = make_measurement(agents=[])
    assert not measurement.start_time
    assert not measurement.end_time


def test_start_end_time_none(make_measurement, make_measurement_agent):
    measurement = make_measurement(agents=[make_measurement_agent()])
    assert not measurement.start_time
    assert not measurement.end_time


def test_start_end_time(make_measurement, make_measurement_agent):
    measurement = make_measurement(
        agents=[
            make_measurement_agent(
                start_time=datetime(2020, 1, 1, 1), end_time=datetime(2020, 1, 1, 3)
            ),
            make_measurement_agent(
                start_time=datetime(2020, 1, 1, 2), end_time=datetime(2020, 1, 1, 4)
            ),
        ]
    )
    assert measurement.start_time == datetime(2020, 1, 1, 1)
    assert measurement.end_time == datetime(2020, 1, 1, 4)


def test_state_no_agents(make_measurement, make_measurement_agent):
    measurement = make_measurement(agents=[])
    assert measurement.state == MeasurementAgentState.Ongoing


def test_state_different(make_measurement, make_measurement_agent):
    measurement = make_measurement(
        agents=[
            make_measurement_agent(state=MeasurementAgentState.Created),
            make_measurement_agent(state=MeasurementAgentState.Finished),
        ]
    )
    assert measurement.state == MeasurementAgentState.Ongoing


def test_state_same(make_measurement, make_measurement_agent):
    measurement = make_measurement(
        agents=[
            make_measurement_agent(state=MeasurementAgentState.Created),
            make_measurement_agent(state=MeasurementAgentState.Created),
        ]
    )
    assert measurement.state == MeasurementAgentState.Created
