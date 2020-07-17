from iris.commons.settings import CommonSettings


class APISettings(CommonSettings):
    """API specific settings."""

    API_DATABASE_HOST: str = "clickhouse"
    API_DATABASE_NAME: str = "iris"

    API_ADMIN_USERNAME: str = "admin"
    API_ADMIN_PASSWORD: str = "admin"
