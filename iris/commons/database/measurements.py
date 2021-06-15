from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from iris.commons.database.database import Database


@dataclass(frozen=True)
class Measurements(Database):
    """Interface that handle measurements history."""

    @property
    def table(self) -> str:
        return self.settings.TABLE_NAME_MEASUREMENTS

    async def create_table(self, drop: bool = False) -> None:
        """Create the table with all registered measurements."""
        if drop:
            await self.call(f"DROP TABLE IF EXISTS {self.table}")

        await self.call(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table}
            (
                uuid       UUID,
                user       String,
                tool       String,
                tags       Array(String),
                state      Enum8('ongoing' = 1, 'finished' = 2, 'canceled' = 3),
                start_time DateTime,
                end_time   Nullable(DateTime)
            )
            ENGINE = MergeTree
            ORDER BY (uuid)
            """
        )

    async def all_count(self, user: str, tag: Optional[str] = None) -> int:
        """Get the count of all results."""
        where_clause = "WHERE user=%(user)s "
        if tag:
            where_clause += f"AND has(tags, '{tag}') "
        response = await self.call(
            f"SELECT Count() FROM {self.table} {where_clause}",
            {"user": user},
        )
        return response[0][0]

    async def all(
        self, user: str, offset: int, limit: int, tag: Optional[str] = None
    ) -> List[dict]:
        """Get all measurements uuid for a given user."""
        where_clause = "WHERE user=%(user)s "
        if tag:
            where_clause += f"AND has(tags, '{tag}') "
        responses = await self.call(
            f"SELECT * FROM {self.table} "
            f"{where_clause}"
            "ORDER BY start_time DESC "
            "LIMIT %(offset)s,%(limit)s",
            {"user": user, "offset": offset, "limit": limit},
        )
        return [self.formatter(response) for response in responses]

    async def get(self, user: str, uuid: UUID) -> Optional[dict]:
        """Get all measurement information."""
        responses = await self.call(
            f"SELECT * FROM {self.table} WHERE user=%(user)s AND uuid=%(uuid)s",
            {"user": user, "uuid": uuid},
        )
        if responses:
            return self.formatter(responses[0])
        return None

    async def register(self, measurement_parameters: dict) -> None:
        """Register a measurement."""
        await self.call(
            f"INSERT INTO {self.table} VALUES",
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

    async def stamp_finished(self, user: str, uuid: UUID) -> None:
        await self.call(
            f"""
            ALTER TABLE {self.table}
            UPDATE state=%(state)s
            WHERE user=%(user)s AND uuid=%(uuid)s
            SETTINGS mutations_sync=1
            """,
            {"state": "finished", "user": user, "uuid": uuid},
        )

    async def stamp_canceled(self, user: str, uuid: UUID) -> None:
        await self.call(
            f"""
            ALTER TABLE {self.table}
            UPDATE state=%(state)s
            WHERE user=%(user)s AND uuid=%(uuid)s
            SETTINGS mutations_sync=1
            """,
            {"state": "canceled", "user": user, "uuid": uuid},
        )

    async def stamp_end_time(self, user: str, uuid: UUID) -> None:
        """Stamp the end time for a measurement."""
        await self.call(
            f"""
            ALTER TABLE {self.table}
            UPDATE end_time=toDateTime(%(end_time)s)
            WHERE user=%(user)s AND uuid=%(uuid)s
            SETTINGS mutations_sync=1
            """,
            {"end_time": datetime.now(), "user": user, "uuid": uuid},
        )

    @staticmethod
    def formatter(row: tuple) -> dict:
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
