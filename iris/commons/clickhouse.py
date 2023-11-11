import asyncio
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from logging import LoggerAdapter
from pathlib import Path
from typing import Any

from diamond_miner.queries import (
    CreateTables,
    DropTables,
    InsertLinks,
    InsertPrefixes,
    Query,
    StoragePolicy,
    links_table,
    prefixes_table,
    results_table,
)
from diamond_miner.subsets import subsets_for
from pych_client import AsyncClickHouseClient, ClickHouseClient

from iris.commons.filesplit import split_compressed_file
from iris.commons.settings import CommonSettings, fault_tolerant


def iter_file(file: str, *, read_size: int = 2**20) -> Iterator[bytes]:
    with open(file, "rb") as f:
        while True:
            chunk = f.read(read_size)
            if not chunk:
                break
            yield chunk


def measurement_id(measurement_uuid: str, agent_uuid: str) -> str:
    return f"{measurement_uuid}__{agent_uuid}"


@dataclass(frozen=True)
class ClickHouse:
    settings: CommonSettings
    logger: LoggerAdapter

    @fault_tolerant
    async def call(self, query: str, params: dict | None = None) -> list[dict]:
        async with AsyncClickHouseClient(**self.settings.clickhouse) as client:
            return await client.json(query, params)

    @fault_tolerant
    async def execute(
        self, query: Query, measurement_id_: str, **kwargs: Any
    ) -> list[dict]:
        with ClickHouseClient(**self.settings.clickhouse) as client:
            return query.execute(client, measurement_id_, **kwargs)

    async def create_tables(
        self,
        measurement_uuid: str,
        agent_uuid: str,
        prefix_len_v4: int,
        prefix_len_v6: int,
        *,
        drop: bool = False,
    ) -> None:
        self.logger.info("Creating tables")
        if drop:
            await self.drop_tables(measurement_uuid, agent_uuid)
        await self.execute(
            CreateTables(
                prefix_len_v4=prefix_len_v4,
                prefix_len_v6=prefix_len_v6,
                storage_policy=StoragePolicy(
                    name=self.settings.CLICKHOUSE_STORAGE_POLICY,
                    archive_to=self.settings.CLICKHOUSE_ARCHIVE_VOLUME,
                    archive_on=datetime.utcnow()
                    + self.settings.CLICKHOUSE_ARCHIVE_INTERVAL,
                ),
            ),
            measurement_id(measurement_uuid, agent_uuid),
        )

    async def drop_tables(self, measurement_uuid: str, agent_uuid: str) -> None:
        self.logger.info("Deleting tables")
        await self.execute(DropTables(), measurement_id(measurement_uuid, agent_uuid))

    async def insert_csv(
        self, measurement_uuid: str, agent_uuid: str, csv_filepath: Path
    ) -> None:
        """Insert CSV file into table."""
        split_dir = csv_filepath.with_suffix(".split")
        split_dir.mkdir(exist_ok=True)

        self.logger.info("Split CSV file")
        split_compressed_file(
            str(csv_filepath),
            str(split_dir / "splitted_"),
            self.settings.CLICKHOUSE_PARALLEL_CSV_MAX_LINE,
            max_estimate_lines=10_000,
            skip_lines=1,
        )

        files = list(split_dir.glob("*"))
        self.logger.info("Number of chunks: %s", len(files))

        concurrency = (os.cpu_count() or 2) // 2
        self.logger.info("Number of concurrent processes: %s", concurrency)

        def insert(file):
            with ClickHouseClient(**self.settings.clickhouse) as client:
                table = results_table(measurement_id(measurement_uuid, agent_uuid))
                query = f"INSERT INTO {table} FORMAT CSV"
                try:
                    client.execute(query, data=iter_file(file))
                finally:
                    os.remove(file)

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(concurrency) as pool:
            await asyncio.gather(
                *[loop.run_in_executor(pool, insert, file) for file in files]
            )
        os.rmdir(split_dir)

    @fault_tolerant
    async def insert_links(self, measurement_uuid: str, agent_uuid: str) -> None:
        """Insert the links in the links' table from the flow view."""
        measurement_id_ = measurement_id(measurement_uuid, agent_uuid)
        await self.call(
            "TRUNCATE {table:Identifier}",
            params={"table": links_table(measurement_id_)},
        )
        with ClickHouseClient(**self.settings.clickhouse) as client:
            query = InsertLinks()
            subsets = subsets_for(query, client, measurement_id_)
            # We limit the number of concurrent requests since this query
            # uses a lot of memory (aggregation of the flows table).
            query.execute_concurrent(
                client,
                measurement_id_,
                subsets=subsets,
                concurrent_requests=8,
            )

    @fault_tolerant
    async def insert_prefixes(self, measurement_uuid: str, agent_uuid: str) -> None:
        """Insert the invalid prefixes in the prefix table."""
        measurement_id_ = measurement_id(measurement_uuid, agent_uuid)
        await self.call(
            "TRUNCATE {table:Identifier}",
            params={"table": prefixes_table(measurement_id_)},
        )
        with ClickHouseClient(**self.settings.clickhouse) as client:
            query = InsertPrefixes()
            subsets = subsets_for(query, client, measurement_id_)
            # We limit the number of concurrent requests since this query
            # uses a lot of memory.
            query.execute_concurrent(
                client,
                measurement_id_,
                subsets=subsets,
                concurrent_requests=8,
            )
