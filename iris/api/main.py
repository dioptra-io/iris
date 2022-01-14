"""API Entrypoint."""
import botocore.exceptions
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.status import HTTP_404_NOT_FOUND
from starlette_exporter import PrometheusMiddleware, handle_metrics

from iris import __version__
from iris.api import agents, measurements, status, targets, users
from iris.commons.dependencies import get_settings

app = FastAPI(
    title="🕸️ Iris",
    description="""
Iris is a system to coordinate complex network measurements from multiple vantage points.<br/>
For more information, please visit our website at [iris.dioptra.io](https://iris.dioptra.io)
or browse the source code on [GitHub](https://github.com/dioptra-io/iris).
""",
    version=__version__,
    openapi_url="/openapi.json",
    docs_url="/docs",
    redoc_url=None,
    contact={"email": "contact@dioptra.io"},
)


app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)

app.include_router(users.router)
app.include_router(agents.router, prefix="/agents", tags=["Agents"])
app.include_router(targets.router, prefix="/targets", tags=["Targets"])
app.include_router(measurements.router, prefix="/measurements", tags=["Measurements"])
app.include_router(status.router, prefix="/status", tags=["Status"])


@app.exception_handler(botocore.exceptions.ClientError)
def botocore_exception_handler(request, exc):
    if exc.response["Error"]["Code"] == "NoSuchKey":
        return Response(status_code=HTTP_404_NOT_FOUND)
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
