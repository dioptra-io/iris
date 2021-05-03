import logging
from multiprocessing import Queue
from typing import Optional

import logging_loki


def create_logger(settings, tags: Optional[dict] = None):
    # Why are there two setLevel() methods?
    # The level set in the logger determines which severity of messages
    # it will pass to its handlers.
    # The level set in each handler determines which messages that handler will send on.

    # Stream handler
    formatter = logging.Formatter(
        "%(asctime)s :: %(levelname)s :: "
        f"{settings.SETTINGS_CLASS.upper()} :: "
        "%(message)s"
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(settings.STREAM_LOGGING_LEVEL)
    stream_handler.setFormatter(formatter)

    # Loki handler
    loki_handler = logging_loki.LokiQueueHandler(
        Queue(settings.LOKI_QUEUE_SIZE),
        url=settings.LOKI_URL,
        version=settings.LOKI_VERSION,
        auth=(settings.LOKI_USER, settings.LOKI_PASSWORD),
        tags=tags,
    )
    loki_handler.setLevel(settings.LOKI_LOGGING_LEVEL)

    # Iris logger
    logger = logging.getLogger(settings.SETTINGS_CLASS)
    logger.setLevel(settings.STREAM_LOGGING_LEVEL)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(stream_handler)
    logger.addHandler(loki_handler)
    logger.propagate = False

    # Diamond-Miner logger
    logger_dm = logging.getLogger("diamond-miner")
    logger_dm.setLevel(settings.STREAM_LOGGING_LEVEL)
    if logger_dm.hasHandlers():
        logger_dm.handlers.clear()
    logger_dm.addHandler(stream_handler)
    logger_dm.addHandler(loki_handler)
    logger_dm.propagate = False

    # Caracal logger
    logger_ca = logging.getLogger("caracal")
    logger_ca.setLevel(settings.STREAM_LOGGING_LEVEL)
    if logger_ca.hasHandlers():
        logger_ca.handlers.clear()
    logger_ca.addHandler(stream_handler)
    logger_ca.addHandler(loki_handler)
    logger_ca.propagate = False

    return logger
