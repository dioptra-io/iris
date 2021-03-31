"""Interfaces with database."""

import json
import logging
import uuid
from datetime import datetime

from aioch import Client
from tenacity import (
    before_sleep_log,
    retry,
    stop_after_delay,
    wait_exponential,
    wait_random,
)

from iris.commons.dataclasses import ParametersDataclass
from iris.commons.subprocess import start_stream_subprocess


def get_session(settings):
    """Get database session."""
    return Client(
        settings.DATABASE_HOST,
        connect_timeout=settings.DATABASE_CONNECT_TIMEOUT,
        send_receive_timeout=settings.DATABASE_SEND_RECEIVE_TIMEOUT,
        sync_request_timeout=settings.DATABASE_SYNC_REQUEST_TIMEOUT,
    )


class Database(object):
    def __init__(self, session, settings, logger=None):
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
            "start_time": row[4].isoformat(),
            "end_time": row[5].isoformat() if row[5] is not None else None,
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
                    "start_time": datetime.fromtimestamp(
                        measurement_parameters["start_time"]
                    ),
                    "end_time": None,
                }
            ],
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
                uuid         UUID,
                user         String,
                version      String,
                hostname     String,
                ip_address   IPv4,
                probing_rate UInt32,
                last_used    DateTime
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
            "probing_rate": row[5],
            "last_used": row[6].isoformat(),
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
                    "probing_rate": parameters["probing_rate"],
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
                targets_file     String,
                probing_rate     UInt32,
                tool_parameters  String,
                finished         UInt8,
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
            "targets_file": row[2],
            "probing_rate": row[3],
            "tool_parameters": json.loads(row[4]),
            "state": "finished" if bool(row[5]) else "ongoing",
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
                    "targets_file": parameters.targets_file,
                    "probing_rate": parameters.probing_rate,
                    "tool_parameters": json.dumps(parameters.tool_parameters),
                    "finished": int(False),
                    "timestamp": datetime.now(),
                }
            ],
        )

    async def stamp_finished(self, measurement_uuid, agent_uuid):
        await self.call(
            f"""
            ALTER TABLE {self.table_name}
            UPDATE finished=%(finished)s
            WHERE measurement_uuid=%(measurement_uuid)s
            AND agent_uuid=%(agent_uuid)s
            """,
            {
                "finished": int(True),
                "measurement_uuid": measurement_uuid,
                "agent_uuid": agent_uuid,
            },
        )


class DatabaseMeasurementResults(Database):
    """Database interface to handle measurement results."""

    def __init__(self, session, settings, table_name, logger=None):
        self.session = session
        self.settings = settings
        self.table_name = table_name
        self.host = session._client.connection.hosts[0][0]
        self.logger = logger

    @staticmethod
    def forge_table_name(
        measurement_uuid,
        agent_uuid,
    ):
        """Forge the table name from measurement UUID and agent UUID."""
        sanitized_measurement_uuid = str(measurement_uuid).replace("-", "_")
        sanitized_agent_uuid = str(agent_uuid).replace("-", "_")
        return f"results__{sanitized_measurement_uuid}" + f"__{sanitized_agent_uuid}"

    @staticmethod
    def parse_table_name(table_name):
        """Parse table name to extract parameters."""
        table_name_split = table_name.split("__")
        measurement_uuid, agent_uuid = (
            table_name_split[1],
            table_name_split[2],
        )
        return {
            "measurement_uuid": measurement_uuid.replace("_", "-"),
            "agent_uuid": agent_uuid.replace("_", "-"),
        }

    def swap_table_name_prefix(self, prefix):
        database_name, table_name = self.table_name.split(".")
        measurement_uuid, agent_uuid = table_name.split("__")[1:3]
        return f"{database_name}.{prefix}__{measurement_uuid}__{agent_uuid}"

    async def create_table(self, drop=False):
        """Create a table."""
        if drop:
            await self.drop_table(self.table_name)

        await self.call(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name}
            (
                probe_src_addr    IPv6,
                probe_dst_addr    IPv6,
                probe_src_port    UInt16,
                probe_dst_port    UInt16,
                probe_ttl_l3      UInt8,
                probe_ttl_l4      UInt8,
                reply_src_addr    IPv6,
                reply_protocol    UInt8,
                reply_icmp_type   UInt8,
                reply_icmp_code   UInt8,
                reply_ttl         UInt8,
                reply_size        UInt16,
                reply_mpls_labels Array(UInt32),
                rtt               Float64,
                round             UInt8,
                -- Materialized columns
                probe_dst_prefix IPv6 MATERIALIZED
                toIPv6(cutIPv6(probe_dst_addr, 8, 1)),
                private_reply_src_addr UInt8 MATERIALIZED
                (
                    reply_src_addr >= toIPv6('10.0.0.0') AND
                    reply_src_addr <= toIPv6('10.255.255.255')
                ) OR
                (
                    reply_src_addr >= toIPv6('172.16.0.0') AND
                    reply_src_addr <= toIPv6('172.31.255.255')
                ) OR
                (
                    reply_src_addr >= toIPv6('192.168.0.0') AND
                    reply_src_addr <= toIPv6('192.168.255.255')
                ) OR
                (
                    reply_src_addr >= toIPv6('fd00::') AND
                    reply_src_addr <= toIPv6('fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')
                )
            )
            ENGINE=MergeTree()
            ORDER BY
            (
                probe_src_addr,
                probe_dst_prefix,
                probe_dst_addr,
                probe_src_port,
                probe_dst_port,
                probe_ttl_l3
            )
            """,
        )

    async def create_materialized_vue_nodes(self):
        await self.call(
            f"""
                CREATE MATERIALIZED VIEW IF NOT EXISTS
                {self.swap_table_name_prefix("nodes")}
                ENGINE = AggregatingMergeTree
                ORDER BY (reply_src_addr)
                AS
                SELECT reply_src_addr,
                    groupUniqArrayState(probe_ttl_l3) AS ttls,
                    avgState(rtt)                     AS avg_rtt,
                    minState(rtt)                     AS min_rtt,
                    maxState(rtt)                     AS max_rtt
                FROM {self.table_name}
                WHERE reply_icmp_type in [3,11]
                AND reply_src_addr != probe_dst_addr
                AND private_reply_src_addr = 0
                GROUP BY reply_src_addr
                SETTINGS optimize_aggregation_in_order = 1
            """
        )

    async def create_materialized_vue_traceroute(self):
        await self.call(
            f"""
            CREATE MATERIALIZED VIEW IF NOT EXISTS
            {self.swap_table_name_prefix("traceroutes")}
            ENGINE = AggregatingMergeTree
            ORDER BY (
                probe_src_addr,
                probe_dst_prefix,
                probe_dst_addr,
                probe_src_port,
                probe_dst_port)
            AS
            SELECT probe_src_addr,
                   probe_dst_prefix,
                   probe_dst_addr,
                   probe_src_port,
                   probe_dst_port,
                   groupArrayInsertAtState(NULL, 32)(reply_src_addr, probe_ttl_l3)
                   AS replies
            FROM {self.table_name}
            WHERE (
                reply_icmp_type IN [3, 11])
                AND(reply_src_addr != probe_dst_addr
                AND private_reply_src_addr = 0
            )
            GROUP BY (
                probe_src_addr,
                probe_dst_prefix,
                probe_dst_addr,
                probe_src_port,
                probe_dst_port)
            SETTINGS optimize_aggregation_in_order = 1
            """
        )

    def formatter(self, row):
        """Database row -> response formater."""
        return {
            "probe_src_addr": str(row[0]),
            "probe_dst_addr": str(row[1]),
            "probe_src_port": row[2],
            "probe_dst_port": row[3],
            "probe_ttl_l3": row[4],
            "probe_ttl_l4": row[5],
            "reply_src_addr": str(row[6]),
            "reply_protocol": row[7],
            "reply_icmp_type": row[8],
            "reply_icmp_code": row[9],
            "reply_ttl": row[10],
            "reply_size": row[11],
            "reply_mpls_labels": row[12],
            "rtt": row[13],
            "round": row[14],
        }

    async def all_count(self):
        """Get the count of all results."""
        response = await self.call(f"SELECT Count() FROM {self.table_name}")
        return response[0][0]

    async def all(self, offset, limit):
        """Get all results given (offset, limit)."""
        response = await self.call(
            f"SELECT * FROM {self.table_name} LIMIT %(offset)s,%(limit)s",
            {"offset": offset, "limit": limit},
        )
        return [self.formatter(row) for row in response]

    async def is_exists(self):
        """Check if table exists."""
        response = await self.call(f"EXISTS TABLE {self.table_name}")
        return bool(response[0][0])

    async def insert_csv(self, csv_filepath):
        """Insert CSV file into table."""
        # We could avoid using clickhouse-client for that,
        # but since we have it for the Reader, why not, at the moment.
        cmd = (
            "cat "
            + str(csv_filepath)
            + " | clickhouse-client --max_insert_block_size=100000 --host="
            + self.host
            + " --query='INSERT INTO "
            + str(self.table_name)
            + " FORMAT CSV'"
        )

        await start_stream_subprocess(
            cmd, stdout=self.logger.info, stderr=self.logger.error
        )
