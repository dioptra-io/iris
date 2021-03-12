import dramatiq

# import ssl

from dramatiq.brokers.redis import RedisBroker

from iris.worker.settings import WorkerSettings


settings = WorkerSettings()
redis_broker = RedisBroker(
    host=settings.REDIS_HOSTNAME,
    port=settings.REDIS_PORT,
    password=settings.REDIS_PASSWORD,
    # ssl=ssl.SSLContext(),
    # ssl_cert_reqs=None,
)
dramatiq.set_broker(redis_broker)
