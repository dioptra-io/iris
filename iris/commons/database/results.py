import asyncio
import os
from asyncio import Semaphore
from dataclasses import dataclass
from datetime import datetime
from ipaddress import IPv6Address
from pathlib import Path
from typing import Generic, List, Optional, TypeVar, Union
from uuid import UUID

import aiofiles.os
from diamond_miner.defaults import PROTOCOLS, UNIVERSE_SUBSET
from diamond_miner.queries import (
    Count,
    CreateTables,
    DropTables,
    GetLinks,
    GetNodes,
    GetPrefixes,
    GetResults,
    InsertLinks,
    InsertPrefixes,
    Query,
    StoragePolicy,
    links_table,
    prefixes_table,
    results_table,
)
from diamond_miner.subsets import subsets_for
from diamond_miner.typing import IPNetwork
from pydantic import IPvAnyAddress

from iris.commons.database.database import Database
from iris.commons.schemas import public
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
                await start_stream_subprocess(
                    f"""
                    {self.database.settings.CLICKHOUSE_CMD} \
                    --database={self.database.settings.DATABASE_NAME} \
                    --host={self.database.settings.DATABASE_HOST} \
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
        subsets = await subsets_for(
            query, self.database.settings.database_url(), self.measurement_id
        )
        # We limit the number of concurrent requests since this query
        # uses a lot of memory (aggregation of the flows table).
        await query.execute_concurrent(
            self.database.settings.database_url(),
            self.measurement_id,
            subsets,
            concurrent_requests=8,
        )

    @fault_tolerant(CommonSettings.database_retry)
    async def insert_prefixes(self) -> None:
        """Insert the invalid prefixes in the prefix table."""
        await self.database.call(f"TRUNCATE {prefixes_table(self.measurement_id)}")
        query = InsertPrefixes()
        subsets = await subsets_for(
            query, self.database.settings.database_url(), self.measurement_id
        )
        # We limit the number of concurrent requests since this query
        # uses a lot of memory.
        await query.execute_concurrent(
            self.database.settings.database_url(),
            self.measurement_id,
            subsets,
            concurrent_requests=8,
        )


@dataclass(frozen=True)
class QueryWrapper(Generic[T]):
    database: Database
    measurement_uuid: Union[str, UUID]
    agent_uuid: Union[str, UUID]
    subset: IPNetwork = UNIVERSE_SUBSET

    def formatter(self, row: tuple) -> T:
        ...

    def query(self) -> Query:
        ...

    def table(self) -> str:
        ...

    @property
    def measurement_id(self) -> str:
        return f"{self.measurement_uuid}__{self.agent_uuid}"

    async def all(self, offset: int, limit: int) -> List[T]:
        response = await self.database.execute(
            self.query(),
            self.measurement_id,
            subsets=(self.subset,),
            limit=(limit, offset),
        )
        return [self.formatter(row) for row in response]

    async def all_count(self) -> int:
        response = await self.database.execute(
            Count(query=self.query()), self.measurement_id, subsets=(self.subset,)
        )
        return int(response[0][0])

    async def exists(self) -> bool:
        response = await self.database.call(f"EXISTS TABLE {self.table()}")
        return bool(response[0][0])


@dataclass(frozen=True)
class Prefixes(QueryWrapper[public.Prefix]):
    """Get measurement prefixes."""

    reply_src_addr_in: Optional[IPNetwork] = None

    def formatter(self, row):
        return public.Prefix(
            prefix=addr_to_string(row[0]),
            has_amplification=bool(row[1]),
            has_loops=bool(row[2]),
        )

    def query(self):
        return GetPrefixes(reply_src_addr_in=self.reply_src_addr_in)

    def table(self):
        return prefixes_table(self.measurement_id)


@dataclass(frozen=True)
class Replies(QueryWrapper[public.Reply]):
    """Get measurement replies."""

    def formatter(self, row: tuple):
        return public.Reply(
            probe_protocol=PROTOCOLS.get(row[0]),
            probe_src_addr=addr_to_string(row[1]),
            probe_dst_addr=addr_to_string(row[2]),
            probe_src_port=row[3],
            probe_dst_port=row[4],
            probe_ttl=row[5],
            quoted_ttl=row[6],
            reply_src_addr=addr_to_string(row[7]),
            reply_protocol=PROTOCOLS.get(row[8]),
            reply_icmp_type=row[9],
            reply_icmp_code=row[10],
            reply_ttl=row[11],
            reply_size=row[12],
            reply_mpls_labels=row[13],
            rtt=round(row[14], 2),
            round=row[15],
        )

    def query(self):
        return GetResults()

    def table(self):
        return results_table(self.measurement_id)


@dataclass(frozen=True)
class Interfaces(QueryWrapper[public.Interface]):
    """Get measurement interfaces."""

    def formatter(self, row: tuple):
        return public.Interface(ttl=row[0], addr=addr_to_string(row[1]))

    def query(self):
        return GetNodes(include_probe_ttl=True)

    def table(self):
        return results_table(self.measurement_id)


@dataclass(frozen=True)
class Links(QueryWrapper[public.Link]):
    """Get measurement links."""

    filter_inter_round: bool = False
    filter_partial: bool = False
    filter_virtual: bool = False
    near_or_far_addr: Optional[IPvAnyAddress] = None

    def formatter(self, row: tuple):
        return public.Link(
            near_ttl=row[0],
            far_ttl=row[1],
            near_addr=addr_to_string(row[2]),
            far_addr=addr_to_string(row[3]),
        )

    def query(self):
        near_or_far_addr = None
        if self.near_or_far_addr:
            near_or_far_addr = str(self.near_or_far_addr)
        return GetLinks(
            include_metadata=True,
            filter_inter_round=self.filter_inter_round,
            filter_partial=self.filter_partial,
            filter_virtual=self.filter_virtual,
            near_or_far_addr=near_or_far_addr,
        )

    def table(self):
        return links_table(self.measurement_id)
