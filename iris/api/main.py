"""API Entrypoint."""

import ssl

from datetime import datetime
from iris import __version__
from iris.api import router
from iris.api.settings import APISettings
from iris.commons.database import get_session, DatabaseMeasurements, DatabaseUsers
from iris.commons.redis import Redis
from iris.commons.storage import Storage
from fastapi import FastAPI
from starlette_exporter import PrometheusMiddleware, handle_metrics


settings = APISettings()
settings_redis_ssl = ssl.SSLContext() if settings.REDIS_SSL else None

app = FastAPI(title="Iris", description="Iris API", version=__version__,)

app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

app.include_router(router, prefix="/v0")


@app.on_event("startup")
async def startup_event():
    # Connect into Redis
    app.redis = Redis()
    await app.redis.connect(
        settings.REDIS_URL, settings.REDIS_PASSWORD, ssl=settings_redis_ssl
    )

    session = get_session(settings.DATABASE_HOST)

    # Create the database on Clickhouse and the measurement table
    database = DatabaseMeasurements(session)
    await database.create_datebase(settings.DATABASE_NAME)
    await database.create_table()

    # Create the users database on Clickhouse and admin user
    database = DatabaseUsers(session)
    await database.create_table()
    admin_user = await database.get(settings.API_ADMIN_USERNAME)
    if admin_user is None:
        await database.register(
            {
                "username": settings.API_ADMIN_USERNAME,
                "email": settings.API_ADMIN_EMAIL,
                "hashed_password": settings.API_ADMIN_HASHED_PASSWORD,
                "is_active": True,
                "is_admin": True,
                "is_full_capable": True,
                "register_date": datetime.now(),
            }
        )

    # Create `targets` bucket in AWS S3 for admin user
    await Storage().create_bucket(
        settings.AWS_S3_TARGETS_BUCKET_PREFIX + settings.API_ADMIN_USERNAME
    )


@app.on_event("shutdown")
async def shutdown_event():
    try:
        await app.redis.disconnect()
    except Exception:
        pass
