from dataclasses import dataclass
from logging import Logger
from typing import Any

from aioch import Client
from diamond_miner.queries import Query

from iris.commons.settings import CommonSettings


@dataclass(frozen=True)
class Database:
    settings: CommonSettings
    logger: Logger

    def fault_tolerant(func):
        async def wrapper(*args, **kwargs):
            retryer = args[0].settings.database_retryer(args[0].logger)
            return await retryer(func)(*args, **kwargs)

        return wrapper

    @fault_tolerant
    async def call(self, *args: Any, default: bool = False, **kwargs: Any):
        return await Client.from_url(self.settings.database_url(default)).execute(
            *args, **kwargs
        )

    @fault_tolerant
    async def execute(self, query: Query, measurement_id: str, **kwargs: Any):
        return await query.execute_async(
            self.settings.database_url(), measurement_id, **kwargs
        )

    async def create_database(self) -> None:
        """Create a database if not exists."""
        await self.call(
            f"CREATE DATABASE IF NOT EXISTS {self.settings.DATABASE_NAME}", default=True
        )
