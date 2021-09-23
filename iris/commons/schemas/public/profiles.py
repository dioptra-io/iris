from datetime import datetime
from typing import Optional

from pydantic import EmailStr, NonNegativeInt
from sqlmodel import Field

from iris.commons.schemas.base import SQLModel


class Profile(SQLModel, table=True):
    """Profile information (Response)."""

    id: Optional[int] = Field(default=None, primary_key=True)
    register_date: datetime = Field(
        default_factory=lambda: datetime.now().replace(microsecond=0)
    )
    username: str
    email: EmailStr
    is_active: bool
    is_admin: bool
    quota: NonNegativeInt
    hashed_password: str
