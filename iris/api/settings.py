"""API settings."""

from iris.commons.settings import CommonSettings


class APISettings(CommonSettings):
    """API specific settings."""

    SETTINGS_CLASS = "api"

    API_ADMIN_USERNAME: str = "admin"
    API_ADMIN_EMAIL: str = "admin@iris.docker.localhost"
    API_ADMIN_QUOTA: int = 64_000_000
    API_ADMIN_HASHED_PASSWORD: str = (
        "$2b$12$DOA6t1HC4zlT/AqFgQcrzuxwcTVAV2HuyZrzxORdBDxhctmMfIbUi"  # noqa
    )
