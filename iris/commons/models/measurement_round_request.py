from iris.commons.models.base import BaseModel
from iris.commons.models.round import Round


class MeasurementRoundRequest(BaseModel):
    measurement_uuid: str
    probe_filename: str
    probing_rate: int | None
    round: Round
