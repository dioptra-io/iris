"""Database interaction."""
import ipaddress

from datetime import datetime
from iris.api.settings import APISettings
from iris.commons.clickhouse import ClickhouseManagement

settings = APISettings()


def packet_formater(row):
    """Database row -> packet formater."""
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


async def get_agents_and_date(client, measurement_uuid):
    """Get UUID of the agents that participated to the measurement + date."""
    agents = []
    date = ""
    response = await client.execute(f"SHOW TABLES FROM {settings.API_DATABASE_NAME}")
    tables = [table[0] for table in response]
    for table in tables:
        table_info = ClickhouseManagement.parse_table_name(table)
        if measurement_uuid == table_info["measurement_uuid"]:
            agents.append(table_info["agent_uuid"])
            date = str(datetime.fromtimestamp(table_info["timestamp"]))
    return agents, date


async def get_table_name(client, measurement_uuid, agent_uuid):
    response = await client.execute(f"SHOW TABLES FROM {settings.API_DATABASE_NAME}")
    tables = [table[0] for table in response]
    for table in tables:
        table_info = ClickhouseManagement.parse_table_name(table)
        if (
            table_info["measurement_uuid"] == measurement_uuid
            and table_info["agent_uuid"] == agent_uuid
        ):
            return table


class MeasurementResults(object):
    """Return measurement results paginated."""

    def __init__(self, request, client, table_name, offset, limit):
        self.table_name = table_name
        self.offset = offset
        self.limit = limit

        self.request = request
        self.client = client

    async def get_count(self):
        """Get total count query."""
        response = await self.client.execute(
            f"SELECT Count() FROM {settings.API_DATABASE_NAME}.{self.table_name} "
        )
        self.count = response[0][0]
        return self.count

    def get_next_url(self) -> str:
        """Constructs `next` parameter in resulting JSON."""
        if self.offset + self.limit >= self.count:
            return None
        return str(
            self.request.url.include_query_params(
                limit=self.limit, offset=self.offset + self.limit
            )
        )

    def get_previous_url(self) -> str:
        """Constructs `previous` parameter in resulting JSON."""
        if self.offset <= 0:
            return None

        if self.offset - self.limit <= 0:
            return str(self.request.url.remove_query_params(keys=["offset"]))

        return str(
            self.request.url.include_query_params(
                limit=self.limit, offset=self.offset - self.limit
            )
        )

    async def get_results(self):
        """Get results from database according to offeset and limit parameters."""
        response = await self.client.execute(
            f"SELECT * FROM {settings.API_DATABASE_NAME}.{self.table_name} "
            f"LIMIT {self.offset},{self.limit}"
        )
        return [packet_formater(row) for row in response]

    async def query(self):
        """Paginate and returns the results."""
        count = await self.get_count()
        results = await self.get_results()
        return {
            "count": count,
            "next": self.get_next_url(),
            "previous": self.get_previous_url(),
            "results": results,
        }
