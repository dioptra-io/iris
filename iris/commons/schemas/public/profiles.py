from typing import Optional
from uuid import UUID

from iris.commons.schemas.base import BaseModel


class RIPEAccount(BaseModel):
    account: Optional[str]
    key: Optional[str]


class Profile(BaseModel):
    """Profile information (Response)."""

    uuid: UUID
    username: str
    email: str
    is_active: bool
    is_admin: bool
    quota: int
    register_date: str
    ripe: RIPEAccount
