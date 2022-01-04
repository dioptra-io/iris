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


# def create_logger(settings):
#     # Why are there two setLevel() methods?
#     # The level set in the logger determines which severity of messages
#     # it will pass to its handlers.
#     # The level set in each handler determines which messages that handler will send on.
#
#     # Stream handler
#     formatter = logging.Formatter(
#         "%(asctime)s :: %(levelname)s :: "
#         f"{settings.SETTINGS_CLASS.upper()} :: "
#         "%(message)s"
#     )
#     stream_handler = logging.StreamHandler()
#     stream_handler.setLevel(settings.STREAM_LOGGING_LEVEL)
#     stream_handler.setFormatter(formatter)
#
#     # Iris logger
#     logger = logging.getLogger(settings.SETTINGS_CLASS)
#     logger.setLevel(settings.STREAM_LOGGING_LEVEL)
#     if logger.hasHandlers():
#         logger.handlers.clear()
#     logger.addHandler(stream_handler)
#     logger.propagate = False
#
#     # Diamond-Miner logger
#     logger_dm = logging.getLogger("diamond-miner")
#     logger_dm.setLevel(settings.STREAM_LOGGING_LEVEL)
#     if logger_dm.hasHandlers():
#         logger_dm.handlers.clear()
#     logger_dm.addHandler(stream_handler)
#     logger_dm.propagate = False
#
#     # Caracal logger
#     logger_ca = logging.getLogger("caracal")
#     logger_ca.setLevel(settings.STREAM_LOGGING_LEVEL)
#     if logger_ca.hasHandlers():
#         logger_ca.handlers.clear()
#     logger_ca.addHandler(stream_handler)
#     logger_ca.propagate = False
#
#     return logger
