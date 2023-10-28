"""API Entrypoint."""
import botocore.exceptions
from fastapi import Depends, FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.status import HTTP_404_NOT_FOUND, HTTP_503_SERVICE_UNAVAILABLE
from starlette_exporter import PrometheusMiddleware, handle_metrics

from iris import __version__
from iris.api import agents, maintenance, measurements, status, targets, users
from iris.api.authentication import current_superuser
from iris.api.settings import APISettings


def make_app(*args, settings: APISettings | None = None) -> FastAPI:
    settings = settings or APISettings()

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
        contact={"email": "iris@dioptra.io"},
        swagger_ui_parameters={
            "displayRequestDuration": True,
            "tryItOutEnabled": True,
        },
    )

    app.add_middleware(PrometheusMiddleware)
    app.add_route("/metrics", handle_metrics)

    app.include_router(users.router)
    app.include_router(agents.router, prefix="/agents", tags=["Agents"])
    app.include_router(targets.router, prefix="/targets", tags=["Targets"])
    app.include_router(
        measurements.router, prefix="/measurements", tags=["Measurements"]
    )
    app.include_router(status.router, prefix="/status", tags=["Status"])
    app.include_router(
        maintenance.router,
        prefix="/maintenance",
        tags=["Maintenance"],
        dependencies=[Depends(current_superuser)],
    )

    if settings.API_CORS_ALLOW_ORIGIN:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.API_CORS_ALLOW_ORIGIN.split(","),
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    if settings.API_READ_ONLY:
        app.add_middleware(ReadOnlyMiddleware)

    @app.exception_handler(botocore.exceptions.ClientError)
    def botocore_exception_handler(request, exc):
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return Response(status_code=HTTP_404_NOT_FOUND)
        raise exc

    return app


class ReadOnlyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if request.method not in ("GET", "HEAD", "OPTIONS"):
            return JSONResponse(
                dict(
                    detail="Iris is under maintenance. Write operations are not available."
                ),
                status_code=HTTP_503_SERVICE_UNAVAILABLE,
            )
        return await call_next(request)
