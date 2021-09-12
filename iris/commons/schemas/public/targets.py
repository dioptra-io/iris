from datetime import datetime
from typing import List

from pydantic import NonNegativeInt

from iris.commons.schemas.base import BaseModel


class TargetSummary(BaseModel):
    """Information about a target (Response)."""

    key: str
    last_modified: datetime


class Target(BaseModel):
    """Information about a target (Response)."""

    key: str
    size: NonNegativeInt
    content: List[str]
    last_modified: datetime


class TargetPostResponse(BaseModel):
    """POST /targets (Response)."""

    key: str
    action: str


class TargetDeleteResponse(BaseModel):
    """DELETE /targets (Response)."""

    key: str
    action: str
