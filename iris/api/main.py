"""API Entrypoint."""

from iris import __version__
from iris.api import router
from iris.api.settings import APISettings
from iris.commons.database import DatabaseMeasurements
from iris.commons.redis import Redis
from iris.commons.storage import Storage
from fastapi import FastAPI
from starlette_exporter import PrometheusMiddleware, handle_metrics


settings = APISettings()

app = FastAPI(title="Iris", description="Iris API", version=__version__,)

app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

app.include_router(router, prefix="/v0")


@app.on_event("startup")
async def startup_event():
    # Connect into Redis
    app.redis = Redis()
    await app.redis.connect(settings.REDIS_URL, settings.REDIS_PASSWORD)

    # Create `targets` bucket in AWS S3
    try:
        await Storage().create_bucket(settings.AWS_S3_TARGETS_BUCKET_NAME)
    except Exception:
        pass

    # Create the database on Clickhouse
    database = DatabaseMeasurements(
        host=settings.DATABASE_HOST, table_name=settings.MEASUREMENTS_TABLE_NAME
    )
    await database.create_datebase(settings.DATABASE_NAME)
    await database.create_table()


@app.on_event("shutdown")
async def shutdown_event():
    try:
        await app.redis.close()
    except Exception:
        pass
