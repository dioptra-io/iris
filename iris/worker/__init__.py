import dramatiq
import logging
import logging_loki

# import ssl

from dramatiq.brokers.redis import RedisBroker
from iris.worker.settings import WorkerSettings
from multiprocessing import Queue

settings = WorkerSettings()
redis_broker = RedisBroker(
    host=settings.REDIS_HOSTNAME,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD,
    # ssl=ssl.SSLContext(),
    # ssl_cert_reqs=None,
)
dramatiq.set_broker(redis_broker)

logger = logging.getLogger("worker")
logger.setLevel(logging.DEBUG if settings.WORKER_DEBUG_MODE else logging.INFO)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: WORKER :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG if settings.WORKER_DEBUG_MODE else logging.INFO)
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
