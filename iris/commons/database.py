"""Interfaces with database."""

import json
import logging
import os
import subprocess
import uuid
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any

from aioch import Client
from diamond_miner.queries import (
    CreateTables,
    DropTables,
    InsertLinks,
    InsertPrefixes,
    Query,
    links_table,
    prefixes_table,
    results_table,
)
from diamond_miner.subsets import subsets_for
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_delay,
    wait_exponential,
    wait_random,
)

from iris.commons.dataclasses import ParametersDataclass
from iris.commons.subprocess import start_stream_subprocess


def get_url(settings, default=False):
    """Get database URL."""
    host = settings.DATABASE_HOST
    database = settings.DATABASE_NAME if not default else "default"
    url = f"clickhouse://{host}/{database}"
    url += f"?connect_timeout={settings.DATABASE_CONNECT_TIMEOUT}"
    url += f"&send_receive_timeout={settings.DATABASE_SEND_RECEIVE_TIMEOUT}"
    url += f"&sync_request_timeout={settings.DATABASE_SYNC_REQUEST_TIMEOUT}"
    return url


def get_session(settings, default=False):
    """Get database session."""
    return Client.from_url(get_url(settings, default))


class Database(object):
    def __init__(self, session, settings, logger=None):
        self.url = get_url(settings)
        self.session = session
        self.settings = settings
        self.logger = logger

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
    async def call(self, *args, **kwargs):
        return await self.session.execute(*args, **kwargs)

    @fault_tolerant
    async def execute(self, query: Query, measurement_id: str, **kwargs: Any):
        return await query.execute_async(self.url, measurement_id, **kwargs)

    @fault_tolerant
    async def execute_concurrent(
        self, query: Query, measurement_id: str, **kwargs: Any
    ):
        return await query.execute_concurrent(self.url, measurement_id, **kwargs)

    async def create_database(self, database_name):
        """Create a database if not exists."""
        await self.call(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    async def drop_table(self, table_name):
        """Drop a table."""
        await self.call(f"DROP TABLE IF EXISTS {table_name}")

    async def clean_table(self, table_name):
        """Clean a table."""
        await self.call(f"ALTER TABLE {table_name} DELETE WHERE 1=1")

    async def disconnect(self):
        """Disconnect agent."""
        await self.session.disconnect()


class DatabaseUsers(Database):
    """Interface that handle users"""

    def __init__(self, session, settings, logger=None):
        super().__init__(session, settings, logger=logger)
        self.table_name = settings.TABLE_NAME_USERS

    async def create_table(self, drop=False):
        """Create the table with all registered users."""
        if drop:
            await self.drop_table(self.table_name)

        await self.call(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            (
                uuid            UUID,
                username        String,
                email           String,
                hashed_password String,
                is_active       UInt8,
                is_admin        UInt8,
                quota           UInt32,
                register_date   DateTime,
                ripe_account    Nullable(String),
                ripe_key        Nullable(String)
            )
            ENGINE=MergeTree()
            ORDER BY (uuid)
            """,
        )

    def formatter(self, row):
        """Database row -> response formater."""
        return {
            "uuid": str(row[0]),
            "username": str(row[1]),
            "email": str(row[2]),
            "hashed_password": str(row[3]),
            "is_active": bool(row[4]),
            "is_admin": bool(row[5]),
            "quota": row[6],
            "register_date": row[7].isoformat(),
            "ripe_account": str(row[8]) if row[8] is not None else None,
            "ripe_key": str(row[9]) if row[9] is not None else None,
        }

    async def get(self, username):
        """Get all measurement information."""
        responses = await self.call(
            f"SELECT * FROM {self.table_name} WHERE username=%(username)s",
            {"username": username},
        )
        try:
            response = responses[0]
        except IndexError:
            return None

        return self.formatter(response)

    async def register(self, parameters):
        """Register a user."""
        await self.call(
            f"INSERT INTO {self.table_name} VALUES",
            [
                {
                    "uuid": uuid.uuid4(),
                    "username": parameters["username"],
                    "email": parameters["email"],
                    "hashed_password": parameters["hashed_password"],
                    "is_active": int(parameters["is_active"]),
                    "is_admin": int(parameters["is_admin"]),
                    "quota": parameters["quota"],
                    "register_date": parameters["register_date"],
                    "ripe_account": None,
                    "ripe_key": None,
                }
            ],
        )

    async def register_ripe(self, username, ripe_account, ripe_key):
        """Register RIPE information of a user."""
        if ripe_account is None or ripe_key is None:
            await self.call(
                f"ALTER TABLE {self.table_name} UPDATE "
                "ripe_account = NULL, ripe_key =  NULL "
                "WHERE username=%(username)s",
                {"username": username},
            )
        else:
            await self.call(
                f""""
                ALTER TABLE {self.table_name}
                UPDATE ripe_account=%(ripe_account)s, ripe_key=%(ripe_key)s
                WHERE username=%(username)s
                """,
                {
                    "ripe_account": ripe_account,
                    "ripe_key": ripe_key,
                    "username": username,
                },
            )


class DatabaseMeasurements(Database):
    """Interface that handle measurements history."""

    def __init__(self, session, settings, logger=None):
        super().__init__(session, settings, logger=logger)
        self.table_name = settings.TABLE_NAME_MEASUREMENTS

    async def create_table(self, drop=False):
        """Create the table with all registered measurements."""
        if drop:
            await self.drop_table(self.table_name)

        await self.call(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            (
                uuid       UUID,
                user       String,
                tool       String,
                tags       Array(String),
                state      Enum8('ongoing' = 1, 'finished' = 2, 'canceled' = 3),
                start_time DateTime,
                end_time   Nullable(DateTime)
            )
            ENGINE=MergeTree()
            ORDER BY (uuid)
            """
        )

    def formatter(self, row):
        """Database row -> response formater."""
        return {
            "uuid": str(row[0]),
            "user": row[1],
            "tool": row[2],
            "tags": row[3],
            "state": row[4],
            "start_time": row[5].isoformat(),
            "end_time": row[6].isoformat() if row[6] is not None else None,
        }

    async def all_count(self, user, tag=None):
        """Get the count of all results."""
        where_clause = "WHERE user=%(user)s "
        if tag:
            where_clause += f"AND has(tags, '{tag}') "
        response = await self.call(
            f"SELECT Count() FROM {self.table_name} {where_clause}",
            {"user": user},
        )
        return response[0][0]

    async def all(self, user, offset, limit, tag=None):
        """Get all measurements uuid for a given user."""
        where_clause = "WHERE user=%(user)s "
        if tag:
            where_clause += f"AND has(tags, '{tag}') "
        responses = await self.call(
            f"SELECT * FROM {self.table_name} "
            f"{where_clause}"
            "ORDER BY start_time DESC "
            "LIMIT %(offset)s,%(limit)s",
            {"user": user, "offset": offset, "limit": limit},
        )
        return [self.formatter(response) for response in responses]

    async def get(self, user, uuid):
        """Get all measurement information."""
        responses = await self.call(
            f"SELECT * FROM {self.table_name} WHERE user=%(user)s AND uuid=%(uuid)s",
            {"user": user, "uuid": uuid},
        )
        try:
            response = responses[0]
        except IndexError:
            return None

        return self.formatter(response)

    async def register(self, measurement_parameters):
        """Register a measurement."""
        await self.call(
            f"INSERT INTO {self.table_name} VALUES",
            [
                {
                    "uuid": measurement_parameters["measurement_uuid"],
                    "user": measurement_parameters["user"],
                    "tool": measurement_parameters["tool"],
                    "tags": measurement_parameters["tags"],
                    "state": "ongoing",
                    "start_time": datetime.fromtimestamp(
                        measurement_parameters["start_time"]
                    ),
                    "end_time": None,
                }
            ],
        )

    async def stamp_finished(self, user, uuid):
        """Stamp the end time for a measurement."""
        await self.call(
            f"""
            ALTER TABLE {self.table_name}
            UPDATE state=%(state)s
            WHERE user=%(user)s AND uuid=%(uuid)s
            """,
            {"state": "finished", "user": user, "uuid": uuid},
        )

    async def stamp_canceled(self, user, uuid):
        """Stamp the end time for a measurement."""
        await self.call(
            f"""
            ALTER TABLE {self.table_name}
            UPDATE state=%(state)s
            WHERE user=%(user)s AND uuid=%(uuid)s
            """,
            {"state": "canceled", "user": user, "uuid": uuid},
        )

    async def stamp_end_time(self, user, uuid):
        """Stamp the end time for a measurement."""
        await self.call(
            f"""
            ALTER TABLE {self.table_name}
            UPDATE end_time=toDateTime(%(end_time)s)
            WHERE user=%(user)s AND uuid=%(uuid)s
            """,
            {"end_time": datetime.now(), "user": user, "uuid": uuid},
        )


class DatabaseAgents(Database):
    """Interface that handle agents history."""

    def __init__(self, session, settings, logger=None):
        super().__init__(session, settings, logger=logger)
        self.table_name = settings.TABLE_NAME_AGENTS

    async def create_table(self, drop=False):
        """Create the table with all registered agents."""
        if drop:
            await self.drop_table(self.table_name)

        await self.call(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            (
                uuid             UUID,
                user             String,
                version          String,
                hostname         String,
                ip_address       IPv4,
                min_ttl          UInt32,
                max_probing_rate UInt32,
                last_used        DateTime
            )
            ENGINE=MergeTree()
            ORDER BY (uuid)
            """,
        )

    def formatter(self, row):
        """Database row -> response formater."""
        return {
            "uuid": str(row[0]),
            "user": row[1],
            "version": row[2],
            "hostname": row[3],
            "ip_address": str(row[4]),
            "min_ttl": row[5],
            "max_probing_rate": row[6],
            "last_used": row[7].isoformat(),
        }

    async def all(self, user="all"):
        """Get all measurements uuid for a given user."""
        responses = await self.call(
            f"SELECT uuid FROM {self.table_name} WHERE user=%(user)s",
            {"user": user},
        )
        return [str(response[0]) for response in responses]

    async def get(self, uuid, user="all"):
        responses = await self.call(
            f"SELECT * FROM {self.table_name} WHERE user=%(user)s AND uuid=%(uuid)s",
            {"user": user, "uuid": uuid},
        )
        try:
            response = responses[0]
        except IndexError:
            return None
        return self.formatter(response)

    async def register(self, uuid, parameters):
        """Register a physical agent."""
        await self.call(
            f"INSERT INTO {self.table_name} VALUES",
            [
                {
                    "uuid": uuid,
                    "user": "all",  # agents shared for all user at the moment
                    "version": parameters["version"],
                    "hostname": parameters["hostname"],
                    "ip_address": parameters["ip_address"],
                    "min_ttl": parameters["min_ttl"],
                    "max_probing_rate": parameters["max_probing_rate"],
                    "last_used": datetime.now(),
                }
            ],
        )

    async def stamp_last_used(self, uuid, user="all"):
        """Stamp the last used for an agent."""
        await self.call(
            f"""
            ALTER TABLE {self.table_name}
            UPDATE last_used=toDateTime(%(last_used)s)
            WHERE user=%(user)s AND uuid=%(uuid)s
            """,
            {"last_used": datetime.now(), "user": user, "uuid": uuid},
        )


class DatabaseAgentsSpecific(Database):
    """Interface that handle agents parameters specific by measurements history."""

    def __init__(self, session, settings, logger=None):
        super().__init__(session, settings, logger=logger)
        self.table_name = settings.TABLE_NAME_AGENTS_SPECIFIC

    async def create_table(self, drop=False):
        """Create the table with all registered agents."""
        if drop:
            await self.drop_table(self.table_name)

        await self.call(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            (
                measurement_uuid UUID,
                agent_uuid       UUID,
                target_file      String,
                probing_rate     Nullable(UInt32),
                tool_parameters  String,
                state            Enum8('ongoing' = 1, 'finished' = 2, 'canceled' = 3),
                timestamp        DateTime
            )
            ENGINE=MergeTree()
            ORDER BY (measurement_uuid, agent_uuid)
            """,
        )

    def formatter(self, row):
        """Database row -> response formater."""
        return {
            "uuid": str(row[1]),
            "target_file": row[2],
            "probing_rate": row[3],
            "tool_parameters": json.loads(row[4]),
            "state": row[5],
        }

    async def all(self, measurement_uuid):
        """Get all measurement information."""
        responses = await self.call(
            f"SELECT * FROM {self.table_name} WHERE measurement_uuid=%(uuid)s",
            {"uuid": measurement_uuid},
        )

        return [self.formatter(response) for response in responses]

    async def get(self, measurement_uuid, agent_uuid):
        """Get measurement information about a agent."""
        responses = await self.call(
            f"SELECT * FROM {self.table_name} "
            "WHERE measurement_uuid=%(measurement_uuid)s "
            "AND agent_uuid=%(agent_uuid)s",
            {"measurement_uuid": measurement_uuid, "agent_uuid": agent_uuid},
        )

        try:
            response = responses[0]
        except IndexError:
            return None

        return self.formatter(response)

    async def register(self, parameters: ParametersDataclass):
        await self.call(
            f"INSERT INTO {self.table_name} VALUES",
            [
                {
                    "measurement_uuid": parameters.measurement_uuid,
                    "agent_uuid": parameters.agent_uuid,
                    "target_file": parameters.target_file,
                    "probing_rate": parameters.probing_rate,
                    "tool_parameters": json.dumps(parameters.tool_parameters),
                    "state": "ongoing",
                    "timestamp": datetime.now(),
                }
            ],
        )

    async def stamp_finished(self, measurement_uuid, agent_uuid):
        await self.call(
            f"""
            ALTER TABLE {self.table_name}
            UPDATE state=%(state)s
            WHERE measurement_uuid=%(measurement_uuid)s
            AND agent_uuid=%(agent_uuid)s
            """,
            {
                "state": "finished",
                "measurement_uuid": measurement_uuid,
                "agent_uuid": agent_uuid,
            },
        )

    async def stamp_canceled(self, measurement_uuid, agent_uuid):
        await self.call(
            f"""
            ALTER TABLE {self.table_name}
            UPDATE state=%(state)s
            WHERE measurement_uuid=%(measurement_uuid)s
            AND agent_uuid=%(agent_uuid)s
            """,
            {
                "state": "canceled",
                "measurement_uuid": measurement_uuid,
                "agent_uuid": agent_uuid,
            },
        )


def sync_insert_csv(database_name, host, table_name, chunk_filepath: Path):
    with chunk_filepath.open("r") as fin:
        command = [
            "clickhouse-client",
            f"--database={database_name}",
            f"--host={host}",
            "-q",
            f"INSERT INTO {table_name} FORMAT CSV",
        ]
        subprocess.call(command, stdin=fin)
    os.remove(str(chunk_filepath))


class DatabaseMeasurementResults(Database):
    """Database interface to handle measurement results."""

    def __init__(self, session, settings, measurement_uuid, agent_uuid, logger=None):
        self.session = session
        self.settings = settings
        self.host = session._client.connection.hosts[0][0]
        self.measurement_id = f"{measurement_uuid}__{agent_uuid}"
        self.logger = logger
        self.logger_prefix = f"{measurement_uuid} :: {agent_uuid} ::"
        super().__init__(session, settings, logger=logger)

    async def create_table(self, drop=False):
        """Create the results table."""
        if drop:
            await self.execute(DropTables(), self.measurement_id)
        await self.execute(CreateTables(), self.measurement_id)

    def formatter(self, row):
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

    async def all_count(self):
        """Get the count of all results."""
        response = await self.call(
            f"SELECT Count() FROM {results_table(self.measurement_id)}"
        )
        return response[0][0]

    async def all(self, offset, limit):
        """Get all results given (offset, limit)."""
        response = await self.call(
            f"SELECT * FROM {results_table(self.measurement_id)} "
            "LIMIT %(offset)s,%(limit)s",
            {"offset": offset, "limit": limit},
        )
        return [self.formatter(row) for row in response]

    async def is_exists(self):
        """Check if table exists."""
        response = await self.call(f"EXISTS TABLE {results_table(self.measurement_id)}")
        return bool(response[0][0])

    async def insert_csv(self, csv_filepath: Path):
        """Insert CSV file into table."""
        if not self.settings.DATABASE_PARALLEL_CSV_INSERT:
            # If the parallel insert option is not activated
            cmd = (
                "zstd --decompress --stdout "
                + str(csv_filepath)
                + " | clickhouse-client "
                + f"--database={self.settings.DATABASE_NAME} "
                + f"--host={self.host}"
                + " --query='INSERT INTO "
                + results_table(self.measurement_id)
                + " FORMAT CSV'"
            )

            await start_stream_subprocess(
                cmd, stdout=self.logger.info, stderr=self.logger.error
            )
        else:
            # Split
            chunks_prefix = ".".join(csv_filepath.name.split(".")[:-1]) + "_"
            chunks_prefix_path = csv_filepath.parent / chunks_prefix
            await start_stream_subprocess(
                f"zstd --decompress --stdout {csv_filepath} | "
                f"split - {chunks_prefix_path} "
                f"-d -l {self.settings.DATABASE_PARALLEL_CSV_MAX_LINE}",
                stdout=self.logger.info,
                stderr=self.logger.error,
            )

            # Select the chunks
            chunks = os.listdir(csv_filepath.parent)
            chunks = [
                csv_filepath.parent / f for f in chunks if f.startswith(chunks_prefix)
            ]

            self.logger.info(f"{self.logger_prefix} Number of chunks: {len(chunks)}")

            # Parallel insert
            with ProcessPoolExecutor() as exe:
                exe.map(
                    partial(
                        sync_insert_csv,
                        self.settings.DATABASE_NAME,
                        self.host,
                        results_table(self.measurement_id),
                    ),
                    chunks,
                )

    async def insert_links(self, round_number):
        """Insert the links in the links table from the flow view."""
        # TODO: `subsets_for` fault-tolerancy
        await self.call(f"TRUNCATE {links_table(self.measurement_id)}")
        query = InsertLinks()
        subsets = await subsets_for(query, self.url, self.measurement_id)
        await self.execute_concurrent(query, self.measurement_id, subsets=subsets)

    async def insert_prefixes(self, round_number):
        """Insert the invalid prefixes in the prefix table."""
        # TODO: `subsets_for` fault-tolerancy
        await self.call(f"TRUNCATE {prefixes_table(self.measurement_id)}")
        query = InsertPrefixes()
        subsets = await subsets_for(query, self.url, self.measurement_id)
        await self.execute_concurrent(query, self.measurement_id, subsets=subsets)
