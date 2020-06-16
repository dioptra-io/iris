import dramatiq
import logging

from dramatiq.brokers.redis import RedisBroker


redis_broker = RedisBroker(host="redis", port=6379)
dramatiq.set_broker(redis_broker)

logger = logging.getLogger("worker")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: WORKER :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate = False
