import dramatiq
from dramatiq.brokers.redis import RedisBroker

from iris.worker.settings import WorkerSettings

settings = WorkerSettings()
redis_broker = RedisBroker(url=settings.REDIS_URL)
dramatiq.set_broker(redis_broker)
