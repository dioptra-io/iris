from datetime import datetime
from typing import List

from pydantic import NonNegativeInt

from iris.commons.models.base import BaseModel


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
