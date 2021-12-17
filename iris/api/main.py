"""API Entrypoint."""
import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics

from iris import __version__
from iris.api import router
from iris.api.dependencies import Base, get_database, get_sqlalchemy, settings
from iris.commons.database import measurements

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
        "email": "iris@dioptra.io",
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

    database_clickhouse = get_database()
    database_sqlalchemy = get_sqlalchemy()

    # Connect to the sqlalchemy database
    await database_sqlalchemy.connect()

    # Create the measurement table
    await measurements.create_table(database_clickhouse)

    # Create the sqlalchemy database
    Base.metadata.create_all(settings.sqlalchemy_engine())


@app.on_event("shutdown")
async def shutdown():
    database_sqlalchemy = get_sqlalchemy()
    await database_sqlalchemy.disconnect()
