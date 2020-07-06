from iris.commons.settings import CommonSettings


class APISettings(CommonSettings):
    """API specific settings."""

    API_DATABASE_HOST: str = "clickhouse"
    API_DATABASE_NAME: str = "iris"
