"""API Entrypoint."""
import botocore.exceptions
from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics

from iris import __version__
from iris.api import agents, measurements, targets, users
from iris.api.dependencies import get_settings

app = FastAPI(
    title="Iris",
    description="Resilient Internet-scale measurement system.",
    version=__version__,
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url=None,
    contact={
        "name": "Iris",
        "url": "https://iris.dioptra.io",
        "email": "iris@dioptra.io",
    },
)


app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

app.include_router(users.router)
app.include_router(agents.router, prefix="/agents", tags=["Agents"])
app.include_router(targets.router, prefix="/targets", tags=["Targets"])
app.include_router(measurements.router, prefix="/measurements", tags=["Measurements"])


@app.exception_handler(botocore.exceptions.ClientError)
def botocore_exception_handler(request, exc):
    if exc.response["Error"]["Code"] == "NoSuchKey":
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    raise exc


@app.on_event("startup")
async def startup_event():
    settings = get_settings()
    if settings.API_CORS_ALLOW_ORIGIN:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.API_CORS_ALLOW_ORIGIN.split(","),
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
