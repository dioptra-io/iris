"""API settings."""

from typing import Optional

from iris.commons.settings import CommonSettings


class APISettings(CommonSettings):
    """API specific settings."""

    API_CORS_ALLOW_ORIGIN: Optional[str] = None
    API_COOKIE_DOMAIN: Optional[str] = None
    API_COOKIE_SAMESITE: str = "lax"
    API_COOKIE_LIFETIME: int = 3600 * 24 * 365  # seconds

    API_JWT_SECRET_KEY: str = (
        "809dc30aafc109be9050e19f90ab8bf5924b8ddae334b2960d55b6a813af90c7"
    )
    API_JWT_LIFETIME: int = 3600  # seconds

    API_OAUTH_GITHUB_CLIENT_ID: str = ""
    API_OAUTH_GITHUB_CLIENT_SECRET: str = ""

    API_READ_ONLY: bool = False
