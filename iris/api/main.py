"""API Entrypoint."""

from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics

from iris import __version__
from iris.api import router
from iris.api.settings import APISettings
from iris.commons.database import DatabaseMeasurements, DatabaseUsers, get_session
from iris.commons.logger import create_logger
from iris.commons.redis import Redis
from iris.commons.storage import Storage

app = FastAPI(
    title="Iris",
    description="Iris API",
    version=__version__,
    openapi_url="/api/openapi.json",
    docs_url="/api/docs",
    redoc_url=None,
)


app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

app.include_router(router, prefix="/api")


@app.on_event("startup")
async def startup_event():
    # Get API settings & logger
    app.settings = APISettings()

    # Add CORS whitelist
    if app.settings.API_CORS_ALLOW_ORIGIN:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[app.settings.API_CORS_ALLOW_ORIGIN],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    app.logger = create_logger(app.settings)
    app.storage = Storage(app.settings, app.logger)

    # Connect into Redis
    app.redis = Redis(settings=app.settings, logger=app.logger)
    await app.redis.connect(app.settings.REDIS_URL, app.settings.REDIS_PASSWORD)

    session = get_session(app.settings)

    # Create the database on Clickhouse and the measurement table
    database = DatabaseMeasurements(session, app.settings, logger=app.logger)
    await database.create_database(app.settings.DATABASE_NAME)
    await database.create_table()

    # Create the users database on Clickhouse and admin user
    database = DatabaseUsers(session, app.settings, logger=app.logger)
    await database.create_table()
    admin_user = await database.get(app.settings.API_ADMIN_USERNAME)
    if admin_user is None:
        await database.register(
            {
                "username": app.settings.API_ADMIN_USERNAME,
                "email": app.settings.API_ADMIN_EMAIL,
                "hashed_password": app.settings.API_ADMIN_HASHED_PASSWORD,
                "is_active": True,
                "is_admin": True,
                "quota": app.settings.API_ADMIN_QUOTA,
                "register_date": datetime.now(),
            }
        )

    # Create `targets` bucket in AWS S3 for admin user
    await app.storage.create_bucket(
        app.settings.AWS_S3_TARGETS_BUCKET_PREFIX + app.settings.API_ADMIN_USERNAME
    )
    # Create `archive` bucket in AWS S3 for admin user
    await app.storage.create_bucket(
        app.settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + app.settings.API_ADMIN_USERNAME
    )


@app.on_event("shutdown")
async def shutdown_event():
    try:
        await app.redis.disconnect()
    except Exception:
        pass
