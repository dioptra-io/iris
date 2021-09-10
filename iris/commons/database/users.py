from dataclasses import dataclass
from typing import Optional

from iris.commons.database.database import Database
from iris.commons.schemas import public


@dataclass(frozen=True)
class Users:
    """Interface that handle users"""

    database: Database

    @property
    def table(self) -> str:
        return self.database.settings.TABLE_NAME_USERS

    async def create_table(self, drop: bool = False) -> None:
        """Create the table with all registered users."""
        if drop:
            await self.database.call(f"DROP TABLE IF EXISTS {self.table}")

        await self.database.call(
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

    async def get(self, username: str) -> Optional[public.Profile]:
        """Get all user information."""
        responses = await self.database.call(
            f"""
            SELECT * FROM {self.table}
            WHERE username=%(username)s
            """,
            {"username": username},
        )
        if responses:
            return self.formatter(responses[0])
        return None

    async def register(self, profile: public.Profile) -> None:
        """Register a user."""
        await self.database.call(
            f"INSERT INTO {self.table} VALUES",
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
                    "ripe_account": profile.ripe.account if profile.ripe else None,
                    "ripe_key": profile.ripe.key if profile.ripe else None,
                }
            ],
        )

    async def register_ripe(
        self, username: str, ripe_account: public.RIPEAccount
    ) -> None:
        await self.database.call(
            f"""
            ALTER TABLE {self.table}
            UPDATE ripe_account=%(ripe_account)s, ripe_key=%(ripe_key)s
            WHERE username=%(username)s
            SETTINGS mutations_sync=1
            """,
            {
                "ripe_account": ripe_account.account,
                "ripe_key": ripe_account.key,
                "username": username,
            },
        )

    async def deregister_ripe(self, username: str):
        await self.database.call(
            f"""
            ALTER TABLE {self.table}
            UPDATE ripe_account=NULL, ripe_key=NULL
            WHERE username=%(username)s
            SETTINGS mutations_sync=1
            """,
            {"username": username},
        )

    @staticmethod
    def formatter(row: tuple) -> public.Profile:
        """Database row -> response formater."""
        ripe = None
        if row[8] and row[9]:
            ripe = public.RIPEAccount(account=row[8], key=row[9])
        profile = public.Profile(
            uuid=row[0],
            username=row[1],
            email=row[2],
            is_active=bool(row[4]),
            is_admin=bool(row[5]),
            quota=row[6],
            register_date=row[7],
            ripe=ripe,
        )
        profile._hashed_password = row[3]
        return profile
