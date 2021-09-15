"""API Entrypoint."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics

from iris import __version__
from iris.api import router
from iris.api.dependencies import get_database, get_storage, settings
from iris.commons.database import measurements, users
from iris.commons.schemas import public

app = FastAPI(
    title="Iris",
    description="Resilient Internet-scale measurement system.",
    version=__version__,
    openapi_url="/api/openapi.json",
    docs_url="/api/docs",
    redoc_url=None,
    contact={
        "name": "Dioptra",
        "url": "https://dioptra.io",
        "email": "contact@dioptra.io",
    },
)


app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

app.include_router(router, prefix="/api")


@app.on_event("startup")
async def startup_event():
    # Add CORS whitelist
    if settings.API_CORS_ALLOW_ORIGIN:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[settings.API_CORS_ALLOW_ORIGIN],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    database = get_database()
    storage = get_storage()

    # Create the database
    await database.create_database()

    # Create the measurement table
    await measurements.create_table(database)

    # Create the users database on Clickhouse and admin user
    await users.create_table(database)
    admin_user = await users.get(database, settings.API_ADMIN_USERNAME)
    if admin_user is None:
        profile = public.Profile(
            username=settings.API_ADMIN_USERNAME,
            email=settings.API_ADMIN_EMAIL,
            is_active=True,
            is_admin=True,
            quota=settings.API_ADMIN_QUOTA,
        )
        profile._hashed_password = settings.API_ADMIN_HASHED_PASSWORD
        await users.register(database, profile)

    # Create `targets` bucket in S3 for admin user
    await storage.create_bucket(
        settings.AWS_S3_TARGETS_BUCKET_PREFIX + settings.API_ADMIN_USERNAME
    )
    # Create `archive` bucket in S3 for admin user
    await storage.create_bucket(
        settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + settings.API_ADMIN_USERNAME
    )
