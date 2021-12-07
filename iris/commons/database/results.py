import asyncio
import os
from asyncio import Semaphore
from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv6Address
from pathlib import Path
from typing import TypeVar, Union
from uuid import UUID

import aiofiles.os
from diamond_miner.queries import (
    CreateTables,
    DropTables,
    InsertLinks,
    InsertPrefixes,
    StoragePolicy,
    links_table,
    prefixes_table,
    results_table,
)
from diamond_miner.subsets import subsets_for

from iris.commons.database.database import Database
from iris.commons.settings import CommonSettings, fault_tolerant
from iris.commons.subprocess import start_stream_subprocess

T = TypeVar("T")


def addr_to_string(addr: IPv6Address) -> str:
    """
    >>> from ipaddress import ip_address
    >>> addr_to_string(ip_address('::dead:beef'))
    '::dead:beef'
    >>> addr_to_string(ip_address('::ffff:8.8.8.8'))
    '8.8.8.8'
    """
    return str(addr.ipv4_mapped or addr)


@dataclass(frozen=True)
class InsertResults:
    """Database interface to handle measurement results."""

    database: Database
    measurement_uuid: Union[str, UUID]
    agent_uuid: Union[str, UUID]
    prefix_len_v4: int
    prefix_len_v6: int

    @property
    def measurement_id(self) -> str:
        return f"{self.measurement_uuid}__{self.agent_uuid}"

    async def create_table(self, drop: bool = False) -> None:
        """Create the results table."""
        if drop:
            await self.database.execute(DropTables(), self.measurement_id)
        await self.database.execute(
            CreateTables(
                prefix_len_v4=self.prefix_len_v4,
                prefix_len_v6=self.prefix_len_v6,
                storage_policy=StoragePolicy(
                    name=self.database.settings.DATABASE_STORAGE_POLICY,
                    archive_to=self.database.settings.DATABASE_ARCHIVE_VOLUME,
                    archive_on=datetime.utcnow()
                    + self.database.settings.DATABASE_ARCHIVE_INTERVAL,
                ),
            ),
            self.measurement_id,
        )

    async def grant_public_access(self) -> None:
        """Grant public access to the tables."""
        await self.database.grant_public_access(results_table(self.measurement_id))
        await self.database.grant_public_access(links_table(self.measurement_id))
        await self.database.grant_public_access(prefixes_table(self.measurement_id))

    async def insert_csv(self, csv_filepath: Path) -> None:
        """Insert CSV file into table."""
        logger_prefix = f"{self.measurement_uuid} :: {self.agent_uuid} ::"

        split_dir = csv_filepath.with_suffix(".split")
        split_dir.mkdir(exist_ok=True)

        await start_stream_subprocess(
            f"""
            {self.database.settings.ZSTD_CMD}  --decompress --stdout {csv_filepath} | \
            {self.database.settings.SPLIT_CMD} --lines={self.database.settings.DATABASE_PARALLEL_CSV_MAX_LINE}
            """,
            stdout=self.database.logger.info,
            stderr=self.database.logger.error,
            cwd=split_dir,
        )

        files = list(split_dir.glob("*"))
        self.database.logger.info(f"{logger_prefix} Number of chunks: {len(files)}")

        concurrency = (os.cpu_count() or 2) // 2
        semaphore = Semaphore(concurrency)
        self.database.logger.info(
            f"{logger_prefix} Number of concurrent processes: {concurrency}"
        )

        async def insert(file):
            async with semaphore:
                password = self.database.settings.DATABASE_PASSWORD
                if not password:
                    password = "''"

                await start_stream_subprocess(
                    f"""
                    {self.database.settings.CLICKHOUSE_CMD} \
                    --database={self.database.settings.DATABASE_NAME} \
                    --host={self.database.settings.DATABASE_HOST} \
                    --user={self.database.settings.DATABASE_USERNAME} \
                    --password {password} \
                    --query='INSERT INTO {results_table(self.measurement_id)} FORMAT CSV' \
                    < {file}
                    """,
                    stdout=self.database.logger.info,
                    stderr=self.database.logger.error,
                )
                await aiofiles.os.remove(file)

        await asyncio.gather(*[insert(file) for file in files])
        await aiofiles.os.rmdir(split_dir)

    @fault_tolerant(CommonSettings.database_retry)
    async def insert_links(self) -> None:
        """Insert the links in the links table from the flow view."""
        await self.database.call(f"TRUNCATE {links_table(self.measurement_id)}")
        query = InsertLinks()
        subsets = subsets_for(
            query, self.database.settings.database_url_http(), self.measurement_id
        )
        # We limit the number of concurrent requests since this query
        # uses a lot of memory (aggregation of the flows table).
        query.execute_concurrent(
            self.database.settings.database_url_http(),
            self.measurement_id,
            subsets,
            concurrent_requests=8,
        )

    @fault_tolerant(CommonSettings.database_retry)
    async def insert_prefixes(self) -> None:
        """Insert the invalid prefixes in the prefix table."""
        await self.database.call(f"TRUNCATE {prefixes_table(self.measurement_id)}")
        query = InsertPrefixes()
        subsets = subsets_for(
            query, self.database.settings.database_url_http(), self.measurement_id
        )
        # We limit the number of concurrent requests since this query
        # uses a lot of memory.
        query.execute_concurrent(
            self.database.settings.database_url_http(),
            self.measurement_id,
            subsets,
            concurrent_requests=8,
        )
