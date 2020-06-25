import dramatiq
import logging
import logging_loki

from dramatiq.brokers.redis import RedisBroker
from diamond_miner.worker.settings import WorkerSettings
from multiprocessing import Queue

settings = WorkerSettings()
redis_broker = RedisBroker(
    host=settings.REDIS_HOSTNAME,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD,
)
dramatiq.set_broker(redis_broker)

logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: WORKER :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

loki_handler = logging_loki.LokiQueueHandler(
    Queue(-1), url=settings.LOKI_URL, version=settings.LOKI_VERSION
)
logger.addHandler(loki_handler)

logger.propagate = False
