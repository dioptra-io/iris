import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from logging import LoggerAdapter
from pathlib import Path
from typing import Any, Iterator, List, Optional

import aiofiles.os
import httpx
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
from httpx import HTTPStatusError

from iris.commons.filesplit import split_compressed_file
from iris.commons.settings import CommonSettings, fault_tolerant


def iter_file(file: str, *, read_size: int = 2 ** 20) -> Iterator[bytes]:
    with open(file, "rb") as f:
        while True:
            chunk = f.read(read_size)
            if not chunk:
                break
            yield chunk


def measurement_id(measurement_uuid: str, agent_uuid: str) -> str:
    return f"{measurement_uuid}__{agent_uuid}"


class QueryError(Exception):
    pass


@dataclass(frozen=True)
class ClickHouse:
    settings: CommonSettings
    logger: LoggerAdapter

    @fault_tolerant
    async def call(
        self,
        query: str,
        *,
        params: Optional[dict] = None,
        timeout=(1, 60),
    ) -> List[dict]:
        # TODO: Cleanup this code and move to a dedicated package?
        query_params = {}
        content = ""

        if params:
            query_params = {f"param_{k}": v for k, v in params.items()}

        params_ = {
            "default_format": "JSONEachRow",
            "query": query,
            **query_params,
        }

        async with httpx.AsyncClient() as client:
            r = await client.post(
                url=self.settings.CLICKHOUSE_URL,
                content=content,
                params=params_,
                timeout=timeout,
            )
            try:
                r.raise_for_status()
                if text := r.text.strip():
                    return [json.loads(line) for line in text.split("\n")]
                return []
            except HTTPStatusError as e:
                raise QueryError(r.content) from e

    @fault_tolerant
    async def execute(self, query: Query, measurement_id_: str, **kwargs: Any):
        return query.execute(self.settings.CLICKHOUSE_URL, measurement_id_, **kwargs)

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

    async def grant_public_access(self, measurement_uuid: str, agent_uuid: str) -> None:
        """Grant public access to the tables."""
        if public_user := self.settings.CLICKHOUSE_PUBLIC_USER:
            self.logger.info("Granting public access to measurement tables")
            measurement_id_ = measurement_id(measurement_uuid, agent_uuid)
            for table in [
                results_table(measurement_id_),
                links_table(measurement_id_),
                prefixes_table(measurement_id_),
            ]:
                # TODO: Proper parameter injection?
                # It doesn't seems to be supported for GRANT.
                # Syntax error: failed at position 17 ('{'): {table:Identifier}
                await self.call(f"GRANT SELECT ON {table} TO {public_user}")

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
            query = f"INSERT INTO {results_table(measurement_id(measurement_uuid, agent_uuid))} FORMAT CSV"
            r = httpx.post(
                self.settings.CLICKHOUSE_URL,
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

    @fault_tolerant
    async def insert_links(self, measurement_uuid: str, agent_uuid: str) -> None:
        """Insert the links in the links' table from the flow view."""
        measurement_id_ = measurement_id(measurement_uuid, agent_uuid)
        await self.call(
            "TRUNCATE {table:Identifier}",
            params={"table": links_table(measurement_id_)},
        )
        query = InsertLinks()
        subsets = subsets_for(query, self.settings.CLICKHOUSE_URL, measurement_id_)
        # We limit the number of concurrent requests since this query
        # uses a lot of memory (aggregation of the flows table).
        query.execute_concurrent(
            self.settings.CLICKHOUSE_URL,
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
        query = InsertPrefixes()
        subsets = subsets_for(query, self.settings.CLICKHOUSE_URL, measurement_id_)
        # We limit the number of concurrent requests since this query
        # uses a lot of memory.
        query.execute_concurrent(
            self.settings.CLICKHOUSE_URL,
            measurement_id_,
            subsets=subsets,
            concurrent_requests=8,
        )
