"""API Entrypoint."""

from diamond_miner import __version__
from diamond_miner.api import router
from diamond_miner.api.settings import APISettings
from diamond_miner.commons.redis import Redis
from diamond_miner.commons.storage import Storage
from fastapi import FastAPI


settings = APISettings()

app = FastAPI(
    title="Diamond-Miner", description="Diamond-Miner API", version=__version__,
)
app.include_router(router, prefix="/v0")


@app.on_event("startup")
async def startup_event():
    app.redis = Redis()
    await app.redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)
    try:
        await Storage().create_bucket(settings.AWS_S3_TARGETS_BUCKET_NAME)
    except Exception:
        pass


@app.on_event("shutdown")
async def shutdown_event():
    try:
        await app.redis.close()
    except Exception:
        pass
