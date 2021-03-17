import logging
from multiprocessing import Queue
from typing import Optional

import logging_loki


def create_logger(settings, tags: Optional[dict] = None):
    logger = logging.getLogger(settings.SETTINGS_CLASS)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s :: %(levelname)s :: "
        f"{settings.SETTINGS_CLASS.upper()} :: "
        "%(message)s"
    )
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    loki_handler = logging_loki.LokiQueueHandler(
        Queue(settings.LOKI_QUEUE_SIZE),
        url=settings.LOKI_URL,
        version=settings.LOKI_VERSION,
        auth=(settings.LOKI_USER, settings.LOKI_PASSWORD),
        tags=tags,
    )
    loki_handler.setLevel(logging.INFO)
    logger.addHandler(loki_handler)

    logger.propagate = False

    return logger
