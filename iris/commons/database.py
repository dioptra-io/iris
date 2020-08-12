"""Interfaces with database."""

import ipaddress

from aioch import Client
from datetime import datetime
from iris.commons.subprocess import start_stream_subprocess
from iris.commons.settings import CommonSettings

settings = CommonSettings()


def get_session(host=settings.DATABASE_HOST):
    """Get database session."""
    return Client(host)


class Database(object):
    def __init__(self, session, logger=None):
        self.session = session
        self.logger = logger

    async def create_datebase(self, database_name=settings.DATABASE_NAME):
        """Create a database if not exists."""
        await self.session.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")

    async def drop_table(self, table_name):
        """Drop a table."""
        await self.session.execute(f"DROP TABLE IF EXISTS {table_name}")

    async def clean_table(self, table_name):
        """Clean a table."""
        await self.session.execute(f"ALTER TABLE {table_name} DELETE WHERE 1=1")

    async def disconnect(self):
        """Disconnect agent."""
        await self.session.disconnect()


class DatabaseMeasurements(Database):
    """Interface that handle measurements history."""

    def __init__(self, host, table_name=settings.TABLE_NAME_MEASUREMENTS):
        super().__init__(host)
        self.table_name = table_name

    async def create_table(self, drop=False):
        """Create the table with all registered measurements."""
        if drop:
            self.drop(self.table_name)

        await self.session.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name}"
            "(uuid UUID, user String, targets_file_key Nullable(String), full UInt8, "
            "protocol String, destination_port UInt16, min_ttl UInt8, max_ttl UInt8, "
            "max_round UInt8, start_time DateTime, "
            "end_time Nullable(DateTime)) "
            "ENGINE=MergeTree() "
            "ORDER BY (uuid)",
        )

    def formatter(self, row):
        """Database row -> response formater."""
        return {
            "uuid": str(row[0]),
            "user": row[1],
            "targets_file_key": row[2],
            "full": bool(row[3]),
            "protocol": row[4],
            "destination_port": row[5],
            "min_ttl": row[6],
            "max_ttl": row[7],
            "max_round": row[8],
            "start_time": row[9].isoformat(),
            "end_time": row[10].isoformat() if row[10] is not None else None,
        }

    async def all_count(self):
        """Get the count of all results."""
        response = await self.session.execute(f"SELECT Count() FROM {self.table_name}")
        return response[0][0]

    async def all(self, user, offset, limit):
        """Get all measurements uuid for a given user."""
        responses = await self.session.execute(
            f"SELECT * FROM {self.table_name} "
            "WHERE user=%(user)s LIMIT %(offset)s,%(limit)s",
            {"user": user, "offset": offset, "limit": limit},
        )
        return [self.formatter(response) for response in responses]

    async def get(self, user, uuid):
        """Get all measurement information."""
        responses = await self.session.execute(
            f"SELECT * FROM {self.table_name} WHERE user=%(user)s AND uuid=%(uuid)s",
            {"user": user, "uuid": uuid},
        )
        try:
            response = responses[0]
        except IndexError:
            return None

        return self.formatter(response)

    async def register(self, agents, measurement_parameters):
        """Register a measurement."""
        await self.session.execute(
            f"INSERT INTO {self.table_name} VALUES",
            [
                {
                    "uuid": measurement_parameters["measurement_uuid"],
                    "user": measurement_parameters["user"],
                    "targets_file_key": measurement_parameters["targets_file_key"],
                    "full": int(measurement_parameters["full"]),
                    "protocol": measurement_parameters["protocol"],
                    "destination_port": measurement_parameters["destination_port"],
                    "min_ttl": measurement_parameters["min_ttl"],
                    "max_ttl": measurement_parameters["max_ttl"],
                    "max_round": measurement_parameters["max_round"],
                    "start_time": datetime.fromtimestamp(
                        measurement_parameters["start_time"]
                    ),
                    "end_time": None,
                }
            ],
        )

    async def stamp_end_time(self, user, uuid):
        """Stamp the end time for a measurement."""
        await self.session.execute(
            f"ALTER TABLE {self.table_name} "
            "UPDATE end_time=toDateTime(%(end_time)s) "
            "WHERE user=%(user)s AND uuid=%(uuid)s",
            {"end_time": datetime.now(), "user": user, "uuid": uuid},
        )


class DatabaseAgents(Database):
    """Interface that handle agents history."""

    def __init__(self, host, table_name=settings.TABLE_NAME_AGENTS):
        super().__init__(host)
        self.table_name = table_name

    async def create_table(self, drop=False):
        """Create the table with all registered agents."""
        if drop:
            self.drop(self.table_name)

        await self.session.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name}"
            "(uuid UUID, user String, version String, hostname String, "
            "ip_address IPv4, probing_rate UInt32, buffer_sniffer_size UInt32, "
            "inf_born UInt32, sup_born UInt32, ips_per_subnet UInt32, "
            "pfring UInt8, last_used DateTime) "
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
            "buffer_sniffer_size": row[6],
            "inf_born": row[7],
            "sup_born": row[8],
            "ips_per_subnet": row[9],
            "pfring": bool(row[10]),
            "last_used": row[11].isoformat(),
        }

    async def all(self, user="all"):
        """Get all measurements uuid for a given user."""
        responses = await self.session.execute(
            f"SELECT uuid FROM {self.table_name} WHERE user=%(user)s", {"user": user},
        )
        return [str(response[0]) for response in responses]

    async def get(self, uuid, user="all"):
        responses = await self.session.execute(
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
        await self.session.execute(
            f"INSERT INTO {self.table_name} VALUES",
            [
                {
                    "uuid": uuid,
                    "user": "all",  # agents share for all user at the moment
                    "version": parameters["version"],
                    "hostname": parameters["hostname"],
                    "ip_address": parameters["ip_address"],
                    "probing_rate": parameters["probing_rate"],
                    "buffer_sniffer_size": parameters["buffer_sniffer_size"],
                    "inf_born": parameters["inf_born"],
                    "sup_born": parameters["sup_born"],
                    "ips_per_subnet": parameters["ips_per_subnet"],
                    "pfring": bool(parameters["pfring"]),
                    "last_used": datetime.now(),
                }
            ],
        )

    async def stamp_last_used(self, uuid, user="all"):
        """Stamp the last used for an agent."""
        await self.session.execute(
            f"ALTER TABLE {self.table_name} "
            "UPDATE last_used=toDateTime(%(last_used)s) "
            "WHERE user=%(user)s AND uuid=%(uuid)s",
            {"last_used": datetime.now(), "user": user, "uuid": uuid},
        )


class DatabaseAgentsInMeasurements(Database):
    """Interface that handle agents parameters specific by measurements history."""

    def __init__(self, host, table_name=settings.TABLE_NAME_AGENTS_IN_MEASUREMENTS):
        super().__init__(host)
        self.table_name = table_name

    async def create_table(self, drop=False):
        """Create the table with all registered agents."""
        if drop:
            self.drop(self.table_name)

        await self.session.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table_name}"
            "(measurement_uuid UUID, agent_uuid UUID, min_ttl UInt8, max_ttl UInt8, "
            "finished UInt8, timestamp DateTime) "
            "ENGINE=MergeTree() "
            "ORDER BY (measurement_uuid)",
        )

    def formatter(self, row):
        """Database row -> response formater."""
        return {
            "uuid": str(row[1]),
            "min_ttl": row[2],
            "max_ttl": row[3],
            "state": "finished" if bool(row[4]) else "ongoing",
        }

    async def all(self, measurement_uuid):
        """Get all measurement information."""
        responses = await self.session.execute(
            f"SELECT * FROM {self.table_name} WHERE measurement_uuid=%(uuid)s",
            {"uuid": measurement_uuid},
        )

        return [self.formatter(response) for response in responses]

    async def get(self, measurement_uuid, agent_uuid):
        """Get measurement information about a agent."""
        responses = await self.session.execute(
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

    async def register(self, measurement_uuid, agent_uuid, min_ttl, max_ttl):
        await self.session.execute(
            f"INSERT INTO {self.table_name} VALUES",
            [
                {
                    "measurement_uuid": measurement_uuid,
                    "agent_uuid": agent_uuid,
                    "min_ttl": min_ttl,
                    "max_ttl": max_ttl,
                    "finished": int(False),
                    "timestamp": datetime.now(),
                }
            ],
        )

    async def stamp_finished(self, measurement_uuid, agent_uuid):
        await self.session.execute(
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


class DatabaseMeasurementResults(object):
    """Database interface to handle measurement results."""

    def __init__(self, session, table_name, logger=None):
        self.session = session
        self.table_name = table_name
        self.host = session._client.connection.hosts[0][0]
        self.logger = logger

    @staticmethod
    def forge_table_name(
        measurement_uuid, agent_uuid,
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
            self.drop(self.table_name)

        await self.session.execute(
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
            "source_ip": str(ipaddress.ip_address(row[0])),
            "destination_prefix": str(ipaddress.ip_address(row[1])),
            "destination_ip": str(ipaddress.ip_address(row[2])),
            "reply_ip": str(ipaddress.ip_address(row[3])),
            "protocol": row[4],
            "source_port": row[5],
            "destination_port": row[6],
            "ttl": row[7],
            "ttl_check": row[8],  # implemented only in UDP
            "type": row[9],
            "code": row[10],
            "rtt": row[11],
            "reply_ttl": row[12],
            "reply_size": row[13],
            "round": row[14],
            # "snapshot": row[14], # Not curently used
        }

    async def all_count(self):
        """Get the count of all results."""
        response = await self.session.execute(f"SELECT Count() FROM {self.table_name}")
        return response[0][0]

    async def all(self, offset, limit):
        """Get all results given (offset, limit)."""
        response = await self.session.execute(
            f"SELECT * FROM {self.table_name} LIMIT %(offset)s,%(limit)s",
            {"offset": offset, "limit": limit},
        )
        return [self.formatter(row) for row in response]

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
