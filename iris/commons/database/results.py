import asyncio
import os
from asyncio import Semaphore
from dataclasses import dataclass
from pathlib import Path
from typing import List, Union
from uuid import UUID

import aiofiles.os
from diamond_miner.queries import (
    CreateTables,
    DropTables,
    InsertLinks,
    InsertPrefixes,
    links_table,
    prefixes_table,
    results_table,
)
from diamond_miner.subsets import subsets_for

from iris.commons.database.database import Database
from iris.commons.subprocess import start_stream_subprocess


@dataclass(frozen=True)
class InsertResults(Database):
    """Database interface to handle measurement results."""

    measurement_uuid: Union[str, UUID]
    agent_uuid: Union[str, UUID]

    @property
    def measurement_id(self) -> str:
        return f"{self.measurement_uuid}__{self.agent_uuid}"

    async def create_table(self, drop: bool = False) -> None:
        """Create the results table."""
        if drop:
            await self.execute(DropTables(), self.measurement_id)
        await self.execute(CreateTables(), self.measurement_id)

    async def insert_csv(self, csv_filepath: Path) -> None:
        """Insert CSV file into table."""
        logger_prefix = f"{self.measurement_uuid} :: {self.agent_uuid} ::"

        split_dir = csv_filepath.with_suffix(".split")
        split_dir.mkdir(exist_ok=True)

        await start_stream_subprocess(
            f"""
            {self.settings.ZSTD_CMD}  --decompress --stdout {csv_filepath} | \
            {self.settings.SPLIT_CMD} --lines={self.settings.DATABASE_PARALLEL_CSV_MAX_LINE}
            """,
            stdout=self.logger.info,
            stderr=self.logger.error,
            cwd=split_dir,
        )

        files = list(split_dir.glob("*"))
        self.logger.info(f"{logger_prefix} Number of chunks: {len(files)}")

        concurrency = (os.cpu_count() or 2) // 2
        semaphore = Semaphore(concurrency)
        self.logger.info(
            f"{logger_prefix} Number of concurrent processes: {concurrency}"
        )

        async def insert(file):
            async with semaphore:
                await start_stream_subprocess(
                    f"""
                    {self.settings.CLICKHOUSE_CMD} \
                    --database={self.settings.DATABASE_NAME} \
                    --host={self.settings.DATABASE_HOST} \
                    --query='INSERT INTO {results_table(self.measurement_id)} FORMAT CSV' \
                    < {file}
                    """,
                    stdout=self.logger.info,
                    stderr=self.logger.error,
                )
                await aiofiles.os.remove(file)

        await asyncio.gather(*[insert(file) for file in files])
        await aiofiles.os.rmdir(split_dir)

    @Database.fault_tolerant
    async def insert_links(self) -> None:
        """Insert the links in the links table from the flow view."""
        await self.call(f"TRUNCATE {links_table(self.measurement_id)}")
        query = InsertLinks()
        subsets = await subsets_for(
            query, self.settings.database_url(), self.measurement_id
        )
        # We limit the number of concurrent requests since this query
        # uses a lot of memory (aggregation of the flows table).
        await query.execute_concurrent(
            self.settings.database_url(),
            self.measurement_id,
            subsets,
            concurrent_requests=8,
        )

    @Database.fault_tolerant
    async def insert_prefixes(self) -> None:
        """Insert the invalid prefixes in the prefix table."""
        await self.call(f"TRUNCATE {prefixes_table(self.measurement_id)}")
        query = InsertPrefixes()
        subsets = await subsets_for(
            query, self.settings.database_url(), self.measurement_id
        )
        # We limit the number of concurrent requests since this query
        # uses a lot of memory.
        await query.execute_concurrent(
            self.settings.database_url(),
            self.measurement_id,
            subsets,
            concurrent_requests=8,
        )


class GetPrefixesResults(Database):
    """Get raw results from database."""

    measurement_uuid: Union[str, UUID]
    agent_uuid: Union[str, UUID]

    @property
    def measurement_id(self) -> str:
        return f"{self.measurement_uuid}__{self.agent_uuid}"

    @staticmethod
    def formatter(row: tuple) -> dict:
        """Database row -> response formater."""
        return {
            "probe_protocol": row[0],
            "probe_src_addr": str(row[1]),
            "probe_dst_addr": str(row[2]),
            "probe_src_port": row[3],
            "probe_dst_port": row[4],
            "probe_ttl": row[5],
            "quoted_ttl": row[6],
            "reply_src_addr": str(row[7]),
            "reply_protocol": row[8],
            "reply_icmp_type": row[9],
            "reply_icmp_code": row[10],
            "reply_ttl": row[11],
            "reply_size": row[12],
            "reply_mpls_labels": row[13],
            "rtt": row[14],
            "round": row[15],
        }

    async def exists(self) -> bool:
        """Check if table exists."""
        response = await self.call(f"EXISTS TABLE {results_table(self.measurement_id)}")
        return bool(response[0][0])

    async def all_count(self) -> int:
        """Get the count of all results."""
        response = await self.call(
            f"SELECT Count() FROM {results_table(self.measurement_id)}"
        )
        return response[0][0]

    async def all(self, offset: int, limit: int) -> List[dict]:
        """Get all results given (offset, limit)."""
        response = await self.call(
            f"SELECT * FROM {results_table(self.measurement_id)} "
            "LIMIT %(offset)s,%(limit)s",
            {"offset": offset, "limit": limit},
        )
        return [self.formatter(row) for row in response]


class GetReplyResults(Database):
    """Get raw results from database."""

    measurement_uuid: Union[str, UUID]
    agent_uuid: Union[str, UUID]

    @property
    def measurement_id(self) -> str:
        return f"{self.measurement_uuid}__{self.agent_uuid}"

    @staticmethod
    def formatter(row: tuple) -> dict:
        """Database row -> response formater."""
        return {
            "probe_protocol": row[0],
            "probe_src_addr": str(row[1]),
            "probe_dst_addr": str(row[2]),
            "probe_src_port": row[3],
            "probe_dst_port": row[4],
            "probe_ttl": row[5],
            "quoted_ttl": row[6],
            "reply_src_addr": str(row[7]),
            "reply_protocol": row[8],
            "reply_icmp_type": row[9],
            "reply_icmp_code": row[10],
            "reply_ttl": row[11],
            "reply_size": row[12],
            "reply_mpls_labels": row[13],
            "rtt": row[14],
            "round": row[15],
        }

    async def exists(self) -> bool:
        """Check if table exists."""
        response = await self.call(f"EXISTS TABLE {results_table(self.measurement_id)}")
        return bool(response[0][0])

    async def all_count(self) -> int:
        """Get the count of all results."""
        response = await self.call(
            f"SELECT Count() FROM {results_table(self.measurement_id)}"
        )
        return response[0][0]

    async def all(self, offset: int, limit: int) -> List[dict]:
        """Get all results given (offset, limit)."""
        response = await self.call(
            f"SELECT * FROM {results_table(self.measurement_id)} "
            "LIMIT %(offset)s,%(limit)s",
            {"offset": offset, "limit": limit},
        )
        return [self.formatter(row) for row in response]


class GetInterfacesResults(Database):
    """Get interfaces results from database."""

    measurement_uuid: Union[str, UUID]
    agent_uuid: Union[str, UUID]

    @property
    def measurement_id(self) -> str:
        return f"{self.measurement_uuid}__{self.agent_uuid}"

    @staticmethod
    def formatter(row: tuple) -> dict:
        """Database row -> response formater."""
        return {
            "probe_protocol": row[0],
            "probe_src_addr": str(row[1]),
            "probe_dst_addr": str(row[2]),
            "probe_src_port": row[3],
            "probe_dst_port": row[4],
            "probe_ttl": row[5],
            "quoted_ttl": row[6],
            "reply_src_addr": str(row[7]),
            "reply_protocol": row[8],
            "reply_icmp_type": row[9],
            "reply_icmp_code": row[10],
            "reply_ttl": row[11],
            "reply_size": row[12],
            "reply_mpls_labels": row[13],
            "rtt": row[14],
            "round": row[15],
        }

    async def exists(self) -> bool:
        """Check if table exists."""
        response = await self.call(f"EXISTS TABLE {results_table(self.measurement_id)}")
        return bool(response[0][0])

    async def all_count(self) -> int:
        """Get the count of all results."""
        response = await self.call(
            f"SELECT Count() FROM {results_table(self.measurement_id)}"
        )
        return response[0][0]

    async def all(self, offset: int, limit: int) -> List[dict]:
        """Get all results given (offset, limit)."""
        response = await self.call(
            f"SELECT * FROM {results_table(self.measurement_id)} "
            "LIMIT %(offset)s,%(limit)s",
            {"offset": offset, "limit": limit},
        )
        return [self.formatter(row) for row in response]


class GetLinksResults(Database):
    """Get links results from database."""

    async def exists(self) -> bool:
        """Check if table exists."""
        response = await self.call(f"EXISTS TABLE {results_table(self.measurement_id)}")
        return bool(response[0][0])

    @staticmethod
    def formatter(row: tuple) -> dict:
        """Database row -> response formater."""
        return {
            "probe_protocol": row[0],
            "probe_src_addr": str(row[1]),
            "probe_dst_addr": str(row[2]),
            "probe_src_port": row[3],
            "probe_dst_port": row[4],
            "probe_ttl": row[5],
            "quoted_ttl": row[6],
            "reply_src_addr": str(row[7]),
            "reply_protocol": row[8],
            "reply_icmp_type": row[9],
            "reply_icmp_code": row[10],
            "reply_ttl": row[11],
            "reply_size": row[12],
            "reply_mpls_labels": row[13],
            "rtt": row[14],
            "round": row[15],
        }

    async def all_count(self) -> int:
        """Get the count of all results."""
        response = await self.call(
            f"SELECT Count() FROM {results_table(self.measurement_id)}"
        )
        return response[0][0]

    async def all(self, offset: int, limit: int) -> List[dict]:
        """Get all results given (offset, limit)."""
        response = await self.call(
            f"SELECT * FROM {results_table(self.measurement_id)} "
            "LIMIT %(offset)s,%(limit)s",
            {"offset": offset, "limit": limit},
        )
        return [self.formatter(row) for row in response]
