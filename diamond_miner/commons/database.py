"""Interface with Clickhouse database."""

from diamond_miner.commons.subprocess import start_stream_subprocess


class Database(object):
    """Database interface."""

    def __init__(self, host, logger):
        self.host = host
        self.logger = logger

    @staticmethod
    def forge_table_name(measurement_uuid, agent_uuid, timestamp):
        """Forge the table name from agent UUID and timestamp."""
        sanitized_measurement_uuid = measurement_uuid.replace("-", "_")
        sanitized_agent_uuid = agent_uuid.replace("-", "_")
        sanitized_timestamp = str(timestamp).replace(".", "_")
        return (
            "results"
            + f"__{sanitized_measurement_uuid}"
            + f"__{sanitized_agent_uuid}"
            + f"__{sanitized_timestamp}"
        )

    @staticmethod
    def parse_table_name(table_name):
        """Parse table name to extract parameters."""
        table_name_split = table_name.split("__")
        measurement_uuid, agent_uuid, timestamp = (
            table_name_split[1],
            table_name_split[2],
            table_name_split[3],
        )
        return {
            "measurement_uuid": measurement_uuid.replace("_", "-"),
            "agent_uuid": agent_uuid.replace("_", "-"),
            "timestamp": float(timestamp.replace("_", ".")),
        }

    async def create_datebase(self, database_name):
        """Create a database if not exists."""
        cmd = (
            "clickhouse-client --host="
            + self.host
            + " --query='CREATE DATABASE IF NOT EXISTS "
            + str(database_name)
            + "'"
        )

        await start_stream_subprocess(cmd, logger=self.logger)

    async def create_table(self, table_name, drop=False):
        """Create a table."""
        if drop:
            self.drop(table_name)

        cmd = (
            "clickhouse-client --host="
            + self.host
            + " --query='CREATE TABLE "
            + str(table_name)
            + "(src_ip UInt32, dst_prefix UInt32, dst_ip UInt32, reply_ip UInt32, "
            + "proto UInt8, src_port UInt16, dst_port UInt16, ttl UInt8, "
            + "ttl_from_udp_length UInt8, type UInt8, "
            + "code UInt8, rtt Float64, reply_ttl UInt8, "
            + "reply_size UInt16, round UInt32, snapshot UInt16) ENGINE=MergeTree() "
            + "ORDER BY (src_ip, dst_prefix, dst_ip, ttl, src_port, dst_port, snapshot)"
            + " '"
        )

        await start_stream_subprocess(cmd)

    async def insert_csv(self, csv_filepath, table_name):
        """Insert CSV file into table."""
        cmd = (
            "cat "
            + str(csv_filepath)
            + " | clickhouse-client --max_insert_block_size=100000 --host="
            + self.host
            + " --query='INSERT INTO "
            + str(table_name)
            + " FORMAT CSV'"
        )

        await start_stream_subprocess(cmd, logger=self.logger)

    async def drop_table(self, table_name):
        """Drop a table."""
        cmd = (
            "clickhouse-client --host="
            + self.host
            + " --query='DROP TABLE "
            + str(table_name)
            + "'"
        )

        await start_stream_subprocess(cmd, logger=self.logger)

    async def clean_table(self, table_name):
        """Clean a table."""
        cmd = (
            "clickhouse-client --host="
            + self.host
            + " --query='ALTER TABLE "
            + str(table_name)
            + " DELETE WHERE 1=1"
            + " '"
        )

        await start_stream_subprocess(cmd)
