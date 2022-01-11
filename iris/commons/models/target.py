from datetime import datetime
from typing import List

from pydantic import NonNegativeInt

from iris.commons.models.base import BaseModel


class TargetSummary(BaseModel):
    """Information about a target (Response)."""

    key: str
    last_modified: datetime

    @classmethod
    def from_s3(cls, d: dict) -> "TargetSummary":
        return TargetSummary(
            key=d["key"],
            last_modified=d["last_modified"],
        )


class Target(BaseModel):
    """Information about a target (Response)."""

    key: str
    size: NonNegativeInt
    content: List[str]
    last_modified: datetime

    @classmethod
    def from_s3(cls, d: dict) -> "Target":
        content = []
        if d["content"]:
            content = [line.strip() for line in d["content"].split()]
        return Target(
            key=d["key"],
            size=d["size"],
            content=content,
            last_modified=d["last_modified"],
        )
