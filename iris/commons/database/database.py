from contextlib import asynccontextmanager
from dataclasses import dataclass
from logging import Logger
from typing import Any

from aioch import Client
from diamond_miner.queries import Query

from iris.commons.settings import CommonSettings, fault_tolerant


@dataclass(frozen=True)
class Database:
    settings: CommonSettings
    logger: Logger

    @asynccontextmanager
    async def client(self, default: bool = False):
        client = Client.from_url(self.settings.database_url(default))
        try:
            yield client
        finally:
            await client.disconnect()

    @fault_tolerant(CommonSettings.database_retry)
    async def call(self, *args: Any, default: bool = False, **kwargs: Any):
        async with self.client(default) as c:
            return await c.execute(*args, **kwargs)

    @fault_tolerant(CommonSettings.database_retry)
    async def execute(self, query: Query, measurement_id: str, **kwargs: Any):
        return await query.execute_async(
            self.settings.database_url(), measurement_id, **kwargs
        )

    async def create_database(self) -> None:
        """Create a database if not exists."""
        await self.call(
            f"CREATE DATABASE IF NOT EXISTS {self.settings.DATABASE_NAME}", default=True
        )

    async def grant_public_access(self, table) -> None:
        """Create a database if not exists."""
        public_user = self.settings.DATABASE_PUBLIC_USER
        if public_user is None:
            return
        await self.call(f"GRANT SELECT ON {table} TO {public_user}")
