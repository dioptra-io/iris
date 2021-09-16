from datetime import datetime
from uuid import UUID, uuid4

from pydantic import EmailStr, Field, NonNegativeInt, PrivateAttr

from iris.commons.schemas.base import BaseModel


class Profile(BaseModel):
    """Profile information (Response)."""

    uuid: UUID = Field(default_factory=uuid4)
    register_date: datetime = Field(
        default_factory=lambda: datetime.now().replace(microsecond=0)
    )
    username: str
    email: EmailStr
    is_active: bool
    is_admin: bool
    quota: NonNegativeInt

    # Fields not exposed in the API
    _hashed_password: str = PrivateAttr()
