import dramatiq
from dramatiq.brokers.redis import RedisBroker

from iris.worker.settings import WorkerSettings

settings = WorkerSettings()
broker = RedisBroker(namespace=settings.REDIS_NAMESPACE, url=settings.REDIS_URL)
dramatiq.set_broker(broker)
