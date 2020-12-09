import logging
import logging_loki

from iris.commons.settings import CommonSettings
from multiprocessing import Queue

settings = CommonSettings()

logger = logging.getLogger("common")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

loki_handler = logging_loki.LokiQueueHandler(
    Queue(settings.LOKI_QUEUE_SIZE),
    url=settings.LOKI_URL,
    version=settings.LOKI_VERSION,
)
loki_handler.setLevel(logging.INFO)
logger.addHandler(loki_handler)

logger.propagate = False
