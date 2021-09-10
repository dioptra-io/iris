from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from pydantic import Field, PrivateAttr

from iris.commons.schemas.base import BaseModel


class RIPEAccount(BaseModel):
    account: str
    key: str


class Profile(BaseModel):
    """Profile information (Response)."""

    uuid: UUID = Field(default_factory=uuid4)
    register_date: datetime = Field(default_factory=datetime.now)
    username: str
    email: str
    is_active: bool
    is_admin: bool
    quota: int
    ripe: Optional[RIPEAccount]

    # Fields not exposed in the API
    _hashed_password: str = PrivateAttr()
