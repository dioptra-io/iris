import logging
from dataclasses import dataclass
from logging import Logger
from typing import Any

from aioch import Client
from diamond_miner.queries import Query
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_delay,
    wait_exponential,
    wait_random,
)

from iris.commons.settings import CommonSettings


@dataclass(frozen=True)
class Database:
    settings: CommonSettings
    logger: Logger

    def fault_tolerant(func):
        """Exponential back-off strategy."""

        async def wrapper(*args, **kwargs):
            cls = args[0]
            settings, logger = cls.settings, cls.logger
            return await retry(
                stop=stop_after_delay(settings.DATABASE_TIMEOUT),
                wait=wait_exponential(
                    multiplier=settings.DATABASE_TIMEOUT_EXPONENTIAL_MULTIPLIERS,
                    min=settings.DATABASE_TIMEOUT_EXPONENTIAL_MIN,
                    max=settings.DATABASE_TIMEOUT_EXPONENTIAL_MAX,
                )
                + wait_random(
                    settings.DATABASE_TIMEOUT_RANDOM_MIN,
                    settings.DATABASE_TIMEOUT_RANDOM_MAX,
                ),
                before_sleep=(
                    before_sleep_log(logger, logging.ERROR) if logger else None
                ),
            )(func)(*args, **kwargs)

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
