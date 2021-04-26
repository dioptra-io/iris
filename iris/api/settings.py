"""API settings."""

from typing import Optional

from iris.commons.settings import CommonSettings


class APISettings(CommonSettings):
    """API specific settings."""

    SETTINGS_CLASS = "api"

    API_CORS_ALLOW_ORIGIN: Optional[str] = None

    API_TOKEN_SECRET_KEY: str = (
        "809dc30aafc109be9050e19f90ab8bf5924b8ddae334b2960d55b6a813af90c7"
    )
    API_TOKEN_ALGORITHM: str = "HS256"
    API_TOKEN_EXPIRATION_TIME: int = 1  # in days

    API_ADMIN_USERNAME: str = "admin"
    API_ADMIN_EMAIL: str = "admin@iris.docker.localhost"
    API_ADMIN_QUOTA: int = 64_000_000
    API_ADMIN_HASHED_PASSWORD: str = (
        "$2b$12$DOA6t1HC4zlT/AqFgQcrzuxwcTVAV2HuyZrzxORdBDxhctmMfIbUi"  # noqa
    )
