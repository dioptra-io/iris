"""Database interaction for handling results."""
import ipaddress

from typing import Optional


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


class MeasurementResults(object):
    """Return measurement results paginated."""

    def __init__(self, request, session, table_name, offset, limit):
        self.table_name = table_name

        self.count = 0
        self.offset = offset
        self.limit = limit

        self.request = request
        self.session = session

    async def get_count(self):
        """Get total count query."""
        response = await self.session.execute(f"SELECT Count() FROM {self.table_name}")
        self.count = response[0][0]
        return self.count

    def get_next_url(self) -> Optional[str]:
        """Constructs `next` parameter in resulting JSON."""
        if self.offset + self.limit >= self.count:
            return None
        return str(
            self.request.url.include_query_params(
                limit=self.limit, offset=self.offset + self.limit
            )
        )

    def get_previous_url(self) -> Optional[str]:
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
        query = f"SELECT * FROM {self.table_name} LIMIT %(offset)s,%(limit)s"

        response = await self.session.execute(
            query, {"offset": self.offset, "limit": self.limit}
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
