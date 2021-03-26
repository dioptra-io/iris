"""API settings."""

from iris.commons.settings import CommonSettings


class APISettings(CommonSettings):
    """API specific settings."""

    SETTINGS_CLASS = "api"

    API_TOKEN_SECRET_KEY = (
        "809dc30aafc109be9050e19f90ab8bf5924b8ddae334b2960d55b6a813af90c7"
    )
    API_TOKEN_ALGORITHM = "HS256"
    API_TOKEN_EXPIRATION_TIME = 30  # in minutes

    API_ADMIN_USERNAME: str = "admin"
    API_ADMIN_EMAIL: str = "admin@iris.docker.localhost"
    API_ADMIN_QUOTA: int = 64_000_000
    API_ADMIN_HASHED_PASSWORD: str = (
        "$2b$12$DOA6t1HC4zlT/AqFgQcrzuxwcTVAV2HuyZrzxORdBDxhctmMfIbUi"  # noqa
    )
