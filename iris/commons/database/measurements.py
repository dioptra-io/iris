"""Interface that handle measurements history."""
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from iris.commons.database.database import Database
from iris.commons.schemas import private, public
from iris.commons.schemas.public import MeasurementState


def table(database: Database) -> str:
    return database.settings.TABLE_NAME_MEASUREMENTS


def formatter(row: dict) -> public.Measurement:
    """Database row -> response formater."""
    return public.Measurement(
        uuid=row["uuid"],
        user_id=row["user_id"],
        tool=public.Tool(row["tool"]),
        tags=row["tags"],
        state=public.MeasurementState(row["state"]),
        start_time=row["start_time"],
        end_time=row["end_time"],
        agents=[],
    )


async def create_table(database: Database, drop: bool = False) -> None:
    """Create the table with all registered measurements."""
    if drop:
        await database.call(
            "DROP TABLE IF EXISTS {table:Identifier}", params={"table": table(database)}
        )

    await database.call(
        """
        CREATE TABLE IF NOT EXISTS {table:Identifier}
        (
            uuid       UUID,
            user_id    UUID,
            tool       String,
            tags       Array(String),
            state      Enum8('ongoing' = 1, 'finished' = 2, 'canceled' = 3),
            start_time DateTime,
            end_time   Nullable(DateTime)
        )
        ENGINE = MergeTree
        ORDER BY (uuid)
        """,
        params={"table": table(database)},
    )


async def all_count(
    database: Database, user_id: Optional[UUID] = None, tag: Optional[str] = None
) -> int:
    """Get the count of all results."""
    query = "SELECT count() FROM {table:Identifier} WHERE 1"
    if user_id:
        query += "\nAND user_id={user_id:UUID}"
    if tag:
        query += "\nAND has(tags, {tag:String})"
    response = await database.call(
        query,
        params={"table": table(database), "user_id": user_id, "tag": tag},
    )
    return int(response[0]["count()"])


async def all(
    database: Database,
    offset: int,
    limit: int,
    user_id: Optional[UUID] = None,
    tag: Optional[str] = None,
) -> List[public.Measurement]:
    """Get all measurements uuid for a given user or a tag."""
    query = "SELECT * FROM {table:Identifier} WHERE 1"
    if user_id:
        query += "\nAND user_id={user_id:UUID}"
    if tag:
        query += "\nAND has(tags, {tag:String})"
    query += """
    ORDER BY start_time DESC
    LIMIT {offset:Int},{limit:Int}
    """
    responses = await database.call(
        query,
        params={
            "table": table(database),
            "user_id": user_id,
            "tag": tag,
            "limit": limit,
            "offset": offset,
        },
    )
    return [formatter(response) for response in responses]


async def get(
    database: Database,
    uuid: UUID,
    user_id: Optional[UUID] = None,
    tag: Optional[str] = None,
) -> Optional[public.Measurement]:
    """Get a measurement information based on its uuid for a given user of a tag."""
    query = "SELECT * FROM {table:Identifier} WHERE uuid = {uuid:UUID}"
    if user_id:
        query += "\nAND user_id={user_id:UUID}"
    if tag:
        query += "\nAND has(tags, {tag:String})"
    responses = await database.call(
        query,
        params={"table": table(database), "uuid": uuid, "user_id": user_id, "tag": tag},
    )
    if responses:
        return formatter(responses[0])
    return None


async def register(
    database: Database, measurement_request: private.MeasurementRequest
) -> None:
    """Register a measurement."""
    await database.call(
        "INSERT INTO {table:Identifier} FORMAT JSONEachRow",
        params={"table": table(database)},
        values=[
            {
                "uuid": measurement_request.uuid,
                "user_id": measurement_request.user_id,
                "tool": measurement_request.tool,
                "tags": measurement_request.tags,
                "state": public.MeasurementState.Ongoing.value,
                "start_time": measurement_request.start_time,
                "end_time": None,
            }
        ],
    )


async def set_state(
    database: Database, user_id: UUID, uuid: UUID, state: MeasurementState
) -> None:
    await database.call(
        """
        ALTER TABLE {table:Identifier}
        UPDATE state={state:String}
        WHERE user_id={user_id:UUID} AND uuid={uuid:UUID}
        SETTINGS mutations_sync=1
        """,
        params={
            "table": table(database),
            "state": state.value,
            "user_id": user_id,
            "uuid": uuid,
        },
    )


async def set_end_time(database: Database, user_id: UUID, uuid: UUID) -> None:
    """Stamp the end time for a measurement."""
    await database.call(
        """
        ALTER TABLE {table:Identifier}
        UPDATE end_time=toDateTime({end_time:String})
        WHERE user_id={user_id:UUID} AND uuid={uuid:UUID}
        SETTINGS mutations_sync=1
        """,
        params={
            "table": table(database),
            "end_time": datetime.utcnow().replace(microsecond=0),
            "user_id": user_id,
            "uuid": uuid,
        },
    )
