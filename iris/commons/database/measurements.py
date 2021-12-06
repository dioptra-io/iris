"""Interface that handle measurements history."""
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from iris.commons.database.database import Database
from iris.commons.schemas import private, public


def table(database: Database) -> str:
    return database.settings.TABLE_NAME_MEASUREMENTS


def formatter(row: tuple) -> public.Measurement:
    """Database row -> response formater."""
    return public.Measurement(
        uuid=row[0],
        username=row[1],
        tool=public.Tool(row[2]),
        tags=row[3],
        state=public.MeasurementState(row[4]),
        start_time=row[5],
        end_time=row[6],
        agents=[],
    )


async def create_table(database: Database, drop: bool = False) -> None:
    """Create the table with all registered measurements."""
    if drop:
        await database.call(f"DROP TABLE IF EXISTS {table(database)}")

    await database.call(
        f"""
        CREATE TABLE IF NOT EXISTS {table(database)}
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


async def all_count(
    database: Database, user: Optional[str] = None, tag: Optional[str] = None
) -> int:
    """Get the count of all results."""
    where_clause = "WHERE 1=1 "
    if user:
        where_clause += "AND user=%(user)s "
    if tag:
        where_clause += f"AND has(tags, '{tag}') "

    response = await database.call(
        f"SELECT Count() FROM {table(database)} {where_clause}",
        {"user": user},
    )
    return int(response[0][0])


async def all(
    database: Database,
    offset: int,
    limit: int,
    user: Optional[str] = None,
    tag: Optional[str] = None,
) -> List[public.Measurement]:
    """Get all measurements uuid for a given user."""
    where_clause = "WHERE 1=1 "
    if user:
        where_clause += "AND user=%(user)s "
    if tag:
        where_clause += f"AND has(tags, '{tag}') "

    responses = await database.call(
        f"SELECT * FROM {table(database)} "
        f"{where_clause}"
        "ORDER BY start_time DESC "
        "LIMIT %(offset)s,%(limit)s",
        {"user": user, "offset": offset, "limit": limit},
    )
    return [formatter(response) for response in responses]


async def get(
    database: Database, user: str, uuid: UUID
) -> Optional[public.Measurement]:
    """Get all measurement information."""
    responses = await database.call(
        f"SELECT * FROM {table(database)} WHERE user=%(user)s AND uuid=%(uuid)s",
        {"user": user, "uuid": uuid},
    )
    if responses:
        return formatter(responses[0])
    return None


async def register(
    database: Database, measurement_request: private.MeasurementRequest
) -> None:
    """Register a measurement."""
    await database.call(
        f"INSERT INTO {table(database)} VALUES",
        [
            {
                "uuid": measurement_request.uuid,
                "user": measurement_request.username,
                "tool": measurement_request.tool,
                "tags": measurement_request.tags,
                "state": public.MeasurementState.Ongoing.value,
                "start_time": measurement_request.start_time,
                "end_time": None,
            }
        ],
    )


async def stamp_finished(database: Database, user: str, uuid: UUID) -> None:
    await database.call(
        f"""
        ALTER TABLE {table(database)}
        UPDATE state=%(state)s
        WHERE user=%(user)s AND uuid=%(uuid)s
        SETTINGS mutations_sync=1
        """,
        {
            "state": public.MeasurementState.Finished.value,
            "user": user,
            "uuid": uuid,
        },
    )


async def stamp_canceled(database: Database, user: str, uuid: UUID) -> None:
    await database.call(
        f"""
        ALTER TABLE {table(database)}
        UPDATE state=%(state)s
        WHERE user=%(user)s AND uuid=%(uuid)s
        SETTINGS mutations_sync=1
        """,
        {
            "state": public.MeasurementState.Canceled.value,
            "user": user,
            "uuid": uuid,
        },
    )


async def stamp_end_time(database: Database, user: str, uuid: UUID) -> None:
    """Stamp the end time for a measurement."""
    await database.call(
        f"""
        ALTER TABLE {table(database)}
        UPDATE end_time=toDateTime(%(end_time)s)
        WHERE user=%(user)s AND uuid=%(uuid)s
        SETTINGS mutations_sync=1
        """,
        {
            "end_time": datetime.utcnow().replace(microsecond=0),
            "user": user,
            "uuid": uuid,
        },
    )
