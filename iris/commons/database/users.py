import uuid
from dataclasses import dataclass
from typing import Optional

from iris.commons.database.database import Database


@dataclass(frozen=True)
class Users(Database):
    """Interface that handle users"""

    @property
    def table(self) -> str:
        return self.settings.TABLE_NAME_USERS

    async def create_table(self, drop: bool = False) -> None:
        """Create the table with all registered users."""
        if drop:
            await self.call(f"DROP TABLE IF EXISTS {self.table}")

        await self.call(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table}
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
            ENGINE = MergeTree
            ORDER BY (uuid)
            """
        )

    async def get(self, username: str) -> Optional[dict]:
        """Get all user information."""
        responses = await self.call(
            f"""
            SELECT * FROM {self.table}
            WHERE username=%(username)s
            """,
            {"username": username},
        )
        if responses:
            return self.formatter(responses[0])
        return None

    async def register(self, parameters: dict) -> None:
        """Register a user."""
        await self.call(
            f"INSERT INTO {self.table} VALUES",
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

    async def register_ripe(
        self, username: str, ripe_account: Optional[str], ripe_key: Optional[str]
    ) -> None:
        """Register RIPE information of a user."""
        if ripe_account is None or ripe_key is None:
            await self.call(
                f"""
                ALTER TABLE {self.table}
                UPDATE ripe_account=NULL, ripe_key=NULL
                WHERE username=%(username)s
                SETTINGS mutations_sync=1
                """,
                {"username": username},
            )
        else:
            await self.call(
                f"""
                ALTER TABLE {self.table}
                UPDATE ripe_account=%(ripe_account)s, ripe_key=%(ripe_key)s
                WHERE username=%(username)s
                SETTINGS mutations_sync=1
                """,
                {
                    "ripe_account": ripe_account,
                    "ripe_key": ripe_key,
                    "username": username,
                },
            )

    @staticmethod
    def formatter(row: tuple) -> dict:
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
