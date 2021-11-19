"""API Entrypoint."""
import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, SQLModel, select
from starlette_exporter import PrometheusMiddleware, handle_metrics

from iris import __version__
from iris.api import router
from iris.api.dependencies import get_database, get_storage, settings
from iris.commons.database import measurements
from iris.commons.schemas.public import Profile

app = FastAPI(
    title="Iris",
    description="Resilient Internet-scale measurement system.",
    version=__version__,
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url=None,
    contact={
        "name": "Dioptra",
        "url": "https://dioptra.io",
        "email": "contact@dioptra.io",
    },
)


app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

app.include_router(router)


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

    # Wait for database to be ready
    await asyncio.sleep(settings.API_WAIT_FOR_DATABASE)

    database = get_database()
    storage = get_storage()

    # Create the SQLModel database
    SQLModel.metadata.create_all(settings.sqlmodel_engine())

    # Create the measurement table
    await measurements.create_table(database)

    # Create the admin user if it doesn't exists
    with Session(settings.sqlmodel_engine()) as session:
        results = session.exec(
            select(Profile).where(Profile.username == settings.API_ADMIN_USERNAME)
        )
        if not results.one_or_none():
            session.add(
                Profile(
                    username=settings.API_ADMIN_USERNAME,
                    email=settings.API_ADMIN_EMAIL,
                    is_active=True,
                    is_admin=True,
                    quota=settings.API_ADMIN_QUOTA,
                    hashed_password=settings.API_ADMIN_HASHED_PASSWORD,
                )
            )
            session.commit()

    # Create `targets` bucket in S3 for admin user
    await storage.create_bucket(
        settings.AWS_S3_TARGETS_BUCKET_PREFIX + settings.API_ADMIN_USERNAME
    )
    # Create `archive` bucket in S3 for admin user
    await storage.create_bucket(
        settings.AWS_S3_ARCHIVE_BUCKET_PREFIX + settings.API_ADMIN_USERNAME
    )
