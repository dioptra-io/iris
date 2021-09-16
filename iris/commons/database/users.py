"""Interface that handle users"""
from typing import Optional

from iris.commons.database.database import Database
from iris.commons.schemas import public


def table(database: Database) -> str:
    return database.settings.TABLE_NAME_USERS


def formatter(row: tuple) -> public.Profile:
    """Database row -> response formater."""
    profile = public.Profile(
        uuid=row[0],
        username=row[1],
        email=row[2],
        is_active=bool(row[4]),
        is_admin=bool(row[5]),
        quota=row[6],
        register_date=row[7],
    )
    profile._hashed_password = row[3]
    return profile


async def create_table(database: Database, drop: bool = False) -> None:
    """Create the table with all registered users."""
    if drop:
        await database.call(f"DROP TABLE IF EXISTS {table(database)}")

    await database.call(
        f"""
        CREATE TABLE IF NOT EXISTS {table(database)}
        (
            uuid            UUID,
            username        String,
            email           String,
            hashed_password String,
            is_active       UInt8,
            is_admin        UInt8,
            quota           UInt32,
            register_date   DateTime
        )
        ENGINE = MergeTree
        ORDER BY (uuid)
        """
    )


async def get(database: Database, username: str) -> Optional[public.Profile]:
    """Get all user information."""
    responses = await database.call(
        f"""
        SELECT * FROM {table(database)}
        WHERE username=%(username)s
        """,
        {"username": username},
    )
    if responses:
        return formatter(responses[0])
    return None


async def register(database: Database, profile: public.Profile) -> None:
    """Register a user."""
    await database.call(
        f"INSERT INTO {table(database)} VALUES",
        [
            {
                "uuid": profile.uuid,
                "username": profile.username,
                "email": profile.email,
                "hashed_password": profile._hashed_password,
                "is_active": int(profile.is_active),
                "is_admin": int(profile.is_admin),
                "quota": profile.quota,
                "register_date": profile.register_date,
            }
        ],
    )
