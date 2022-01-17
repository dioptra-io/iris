import logging

base_logger = logging.getLogger("iris")


class Adapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        parts = [self.extra["component"].upper()]
        if uuid := self.extra.get("measurement_uuid"):
            parts.append(uuid)
        if uuid := self.extra.get("agent_uuid"):
            parts.append(uuid)
        parts.append(msg)
        return " :: ".join(parts), kwargs
