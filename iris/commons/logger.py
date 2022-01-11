import logging
import traceback

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


def log_traceback(logger):
    traceback_content = traceback.format_exc()
    for line in traceback_content.splitlines():
        logger.critical(line)
