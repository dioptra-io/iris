import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv6Address
from pathlib import Path
from typing import Iterator, TypeVar

import aiofiles.os
import httpx
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
from httpx import HTTPStatusError

from iris.commons.clickhouse import ClickHouse, QueryError
from iris.commons.filesplit import split_compressed_file
from iris.commons.logger import Adapter, base_logger
from iris.commons.settings import CommonSettings, fault_tolerant

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


def iter_file(file: str, *, read_size: int = 2 ** 20) -> Iterator[bytes]:
    with open(file, "rb") as f:
        while True:
            chunk = f.read(read_size)
            if not chunk:
                break
            yield chunk


@dataclass(frozen=True)
class InsertResults:
    """Database interface to handle measurement results."""

    clickhouse: ClickHouse
    measurement_uuid: str
    agent_uuid: str
    prefix_len_v4: int
    prefix_len_v6: int

    @property
    def measurement_id(self) -> str:
        return f"{self.measurement_uuid}__{self.agent_uuid}"

    async def create_table(self, drop: bool = False) -> None:
        """Create the results table."""
        if drop:
            await self.clickhouse.execute(DropTables(), self.measurement_id)
        await self.clickhouse.execute(
            CreateTables(
                prefix_len_v4=self.prefix_len_v4,
                prefix_len_v6=self.prefix_len_v6,
                storage_policy=StoragePolicy(
                    name=self.clickhouse.settings.CLICKHOUSE_STORAGE_POLICY,
                    archive_to=self.clickhouse.settings.CLICKHOUSE_ARCHIVE_VOLUME,
                    archive_on=datetime.utcnow()
                    + self.clickhouse.settings.CLICKHOUSE_ARCHIVE_INTERVAL,
                ),
            ),
            self.measurement_id,
        )

    async def grant_public_access(self) -> None:
        """Grant public access to the tables."""
        await self.clickhouse.grant_public_access(results_table(self.measurement_id))
        await self.clickhouse.grant_public_access(links_table(self.measurement_id))
        await self.clickhouse.grant_public_access(prefixes_table(self.measurement_id))

    async def insert_csv(self, csv_filepath: Path) -> None:
        """Insert CSV file into table."""
        # TODO: Actually use this logger?
        logger = Adapter(
            base_logger,
            dict(
                component="worker",
                measurement_uuid=self.measurement_uuid,
                agent_uuid=self.agent_uuid,
            ),
        )

        split_dir = csv_filepath.with_suffix(".split")
        split_dir.mkdir(exist_ok=True)

        split_compressed_file(
            str(csv_filepath),
            str(split_dir / "splitted_"),
            self.clickhouse.settings.CLICKHOUSE_PARALLEL_CSV_MAX_LINE,
            max_estimate_lines=10_000,
        )

        files = list(split_dir.glob("*"))
        self.clickhouse.logger.info("Number of chunks: %s", len(files))

        concurrency = (os.cpu_count() or 2) // 2
        self.clickhouse.logger.info("Number of concurrent processes: %s", concurrency)

        def insert(file):
            query = (
                f"INSERT INTO {results_table(self.measurement_id)} FORMAT CSVWithNames"
            )
            r = httpx.post(
                self.clickhouse.settings.CLICKHOUSE_URL,
                content=iter_file(file),
                params={"query": query},
            )
            os.remove(file)
            try:
                r.raise_for_status()
            except HTTPStatusError as e:
                raise QueryError(r.content) from e

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(concurrency) as pool:
            await asyncio.gather(
                *[loop.run_in_executor(pool, insert, file) for file in files]
            )
        await aiofiles.os.rmdir(split_dir)

    @fault_tolerant(CommonSettings.database_retry)
    async def insert_links(self) -> None:
        """Insert the links in the links table from the flow view."""
        await self.clickhouse.call(
            "TRUNCATE {table:Identifier}",
            params={"table": links_table(self.measurement_id)},
        )
        query = InsertLinks()
        subsets = subsets_for(
            query, self.clickhouse.settings.CLICKHOUSE_URL, self.measurement_id
        )
        # We limit the number of concurrent requests since this query
        # uses a lot of memory (aggregation of the flows table).
        query.execute_concurrent(
            self.clickhouse.settings.CLICKHOUSE_URL,
            self.measurement_id,
            subsets,
            concurrent_requests=8,
        )

    @fault_tolerant(CommonSettings.database_retry)
    async def insert_prefixes(self) -> None:
        """Insert the invalid prefixes in the prefix table."""
        await self.clickhouse.call(
            "TRUNCATE {table:Identifier}",
            params={"table": prefixes_table(self.measurement_id)},
        )
        query = InsertPrefixes()
        subsets = subsets_for(
            query, self.clickhouse.settings.CLICKHOUSE_URL, self.measurement_id
        )
        # We limit the number of concurrent requests since this query
        # uses a lot of memory.
        query.execute_concurrent(
            self.clickhouse.settings.CLICKHOUSE_URL,
            self.measurement_id,
            subsets,
            concurrent_requests=8,
        )
