"""Interfaces with database."""

import ipaddress
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
            f"CREATE TABLE IF NOT EXISTS {self.table_name}"
            "(uuid UUID, username String, email String, hashed_password String, "
            "is_active UInt8, is_admin UInt8, quota UInt32, "
            "register_date DateTime, "
            "ripe_account Nullable(String), ripe_key Nullable(String)) "
            "ENGINE=MergeTree() "
            "ORDER BY (uuid)",
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
                f"ALTER TABLE {self.table_name} UPDATE "
                "ripe_account = %(ripe_account)s, ripe_key = %(ripe_key)s "
                "WHERE username=%(username)s",
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
            f"CREATE TABLE IF NOT EXISTS {self.table_name}"
            "(uuid UUID, user String, tool String, tags Array(String), "
            "start_time DateTime, end_time Nullable(DateTime)) "
            "ENGINE=MergeTree() "
            "ORDER BY (uuid)",
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

    async def all_count(self, user):
        """Get the count of all results."""
        response = await self.call(
            f"SELECT Count() FROM {self.table_name} WHERE user=%(user)s",
            {"user": user},
        )
        return response[0][0]

    async def all(self, user, offset, limit):
        """Get all measurements uuid for a given user."""
        responses = await self.call(
            f"SELECT * FROM {self.table_name} "
            "WHERE user=%(user)s "
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
            f"ALTER TABLE {self.table_name} "
            "UPDATE end_time=toDateTime(%(end_time)s) "
            "WHERE user=%(user)s AND uuid=%(uuid)s",
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
            f"CREATE TABLE IF NOT EXISTS {self.table_name}"
            "(uuid UUID, user String, version String, hostname String, ip_address IPv4,"
            "probing_rate UInt32, last_used DateTime) "
            "ENGINE=MergeTree() "
            "ORDER BY (uuid)",
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
            f"ALTER TABLE {self.table_name} "
            "UPDATE last_used=toDateTime(%(last_used)s) "
            "WHERE user=%(user)s AND uuid=%(uuid)s",
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
            f"CREATE TABLE IF NOT EXISTS {self.table_name}"
            "(measurement_uuid UUID, agent_uuid UUID, targets_file String, "
            "probing_rate UInt32, tool_parameters String, "
            "finished UInt8, timestamp DateTime) "
            "ENGINE=MergeTree() "
            "ORDER BY (measurement_uuid, agent_uuid)",
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
            f"ALTER TABLE {self.table_name} "
            "UPDATE finished=%(finished)s "
            "WHERE measurement_uuid=%(measurement_uuid)s "
            "AND agent_uuid=%(agent_uuid)s",
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
        return (
            "results" + f"__{sanitized_measurement_uuid}" + f"__{sanitized_agent_uuid}"
        )

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

    async def create_table(self, drop=False):
        """Create a table."""
        if drop:
            await self.drop_table(self.table_name)

        await self.call(
            f"CREATE TABLE IF NOT EXISTS {self.table_name}"
            "(src_ip UInt32, dst_prefix UInt32, dst_ip UInt32, reply_ip UInt32, "
            "proto UInt8, src_port UInt16, dst_port UInt16, ttl UInt8, "
            "ttl_from_udp_length UInt8, type UInt8, "
            "code UInt8, rtt Float64, reply_ttl UInt8, "
            "reply_size UInt16, round UInt32, snapshot UInt16) ENGINE=MergeTree() "
            "ORDER BY (src_ip, dst_prefix, dst_ip, ttl, src_port, dst_port, snapshot)",
        )

    def formatter(self, row):
        """Database row -> response formater."""
        return {
            "source_ip": str(ipaddress.ip_address(row[0])),  # probe_src_addr
            "destination_prefix": str(ipaddress.ip_address(row[1])),  # calculated
            "destination_ip": str(ipaddress.ip_address(row[2])),  # probe_dst_addr
            "reply_ip": str(ipaddress.ip_address(row[3])),  # reply_src_addr
            "protocol": row[4],  # reply_protocol
            "source_port": row[5],  # probe_src_port
            "destination_port": row[6],  # probe_dst_port
            "ttl": row[7],  # probe_ttl_l3
            "ttl_check": row[8],  # probe_ttl_l4 /
            "type": row[9],  # reply_icmp_code
            "code": row[10],  # reply_icmp_type
            "rtt": row[11],  # rtt
            "reply_ttl": row[12],  # reply_ttl
            "reply_size": row[13],  # reply_size
            "round": row[14],  # round
            "snapshot": row[15],  # N/A
        }

    def csv_formatter(self, row):
        """Database csv -> row formater."""
        return {
            "src_ip": int(row[0]),
            "dst_prefix": int(row[1]),
            "dst_ip": int(row[2]),
            "reply_ip": int(row[3]),
            "proto": int(row[4]),
            "src_port": int(row[5]),
            "dst_port": int(row[6]),
            "ttl": int(row[7]),
            "ttl_from_udp_length": int(row[8]),  # implemented only in UDP
            "type": int(row[9]),
            "code": int(row[10]),
            "rtt": float(row[11]),
            "reply_ttl": int(row[12]),
            "reply_size": int(row[13]),
            "round": int(row[14]),
            "snapshot": int(row[15]),  # Not curently used
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
