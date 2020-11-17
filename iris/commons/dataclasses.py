class ParametersDataclass(object):
    def __init__(
        self,
        agent_uuid,
        measurement_parameters,
        physical_parameters,
        specific_parameters,
    ):
        self.measurement_parameters = measurement_parameters
        self.physical_parameters = physical_parameters
        self.specific_parameters = specific_parameters
        self._dataclass = {
            **physical_parameters,
            **measurement_parameters,
            **{k: v for k, v in specific_parameters.items() if v is not None},
            **{"agent_uuid": agent_uuid},
        }

    def __getattr__(self, parameter):
        try:
            return self._dataclass[parameter]
        except KeyError:
            raise AttributeError(f"`{parameter}` not found in dataclass")

    def to_dict(self):
        return self._dataclass
