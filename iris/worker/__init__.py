import dramatiq
from dramatiq.brokers.redis import RedisBroker

from iris.worker.settings import WorkerSettings

settings = WorkerSettings()
redis_broker = RedisBroker(namespace=settings.REDIS_NAMESPACE, url=settings.REDIS_URL)
dramatiq.set_broker(redis_broker)
dramatiq.set_encoder(dramatiq.PickleEncoder())
