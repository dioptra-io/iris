"""API Entrypoint."""
import time

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from httpx import HTTPStatusError
from sqlmodel import SQLModel
from starlette_exporter import PrometheusMiddleware, handle_metrics

from iris import __version__
from iris.api import router
from iris.api.dependencies import Base, get_database, get_sqlalchemy, settings

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

# TODO
from iris.commons.schemas.agents2 import AgentDatabase

# TODO: Also init test database in conftest.py


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

    settings.sqlalchemy_database_path().parent.mkdir(parents=True, exist_ok=True)

    while True:
        try:
            httpx.get(
                settings.DATABASE_URL, params={"query": "SELECT 1"}, timeout=1
            ).raise_for_status()
            break
        except HTTPStatusError:
            print("Waiting for database...")
            time.sleep(1)

    database_sqlalchemy = get_sqlalchemy()

    # Connect to the sqlalchemy database
    await database_sqlalchemy.connect()

    # Create the sqlalchemy database
    Base.metadata.create_all(settings.sqlalchemy_engine())
    SQLModel.metadata.create_all(settings.sqlalchemy_engine())


@app.on_event("shutdown")
async def shutdown():
    database_sqlalchemy = get_sqlalchemy()
    await database_sqlalchemy.disconnect()
