from iris.commons.models.base import BaseSQLModel
from iris.commons.models.measurement import Measurement
from iris.commons.models.measurement_agent import MeasurementAgent
from iris.commons.models.round import Round


class MeasurementRoundRequest(BaseSQLModel):
    measurement: Measurement
    measurement_agent: MeasurementAgent
    probe_filename: str
    round: Round
