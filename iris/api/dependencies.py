from iris.api.settings import APISettings
from iris.commons.database import Database
from iris.commons.logger import create_logger
from iris.commons.redis import Redis
from iris.commons.storage import Storage

settings = APISettings()
logger = create_logger(settings)


def get_database():
    return Database(settings, logger)


async def get_redis():
    client = await settings.redis_client()
    try:
        yield Redis(client, settings, logger)
    finally:
        await client.close()


def get_storage():
    return Storage(settings, logger)
