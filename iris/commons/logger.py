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
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)

    # Loki handler
    loki_handler = logging_loki.LokiQueueHandler(
        Queue(settings.LOKI_QUEUE_SIZE),
        url=settings.LOKI_URL,
        version=settings.LOKI_VERSION,
        auth=(settings.LOKI_USER, settings.LOKI_PASSWORD),
        tags=tags,
    )
    loki_handler.setLevel(logging.INFO)

    # Iris logger
    logger = logging.getLogger(settings.SETTINGS_CLASS)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    logger.addHandler(loki_handler)
    logger.propagate = False

    # Diamond-Miner logger
    logger_dm = logging.getLogger("diamond-miner")
    logger_dm.setLevel(logging.DEBUG)
    logger_dm.addHandler(stream_handler)
    logger_dm.addHandler(loki_handler)

    return logger
