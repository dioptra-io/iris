from typing import List

from pydantic import BaseModel


class TargetSummary(BaseModel):
    """Information about a target (Response)."""

    key: str
    last_modified: str


class Target(BaseModel):
    """Information about a target (Response)."""

    key: str
    size: int
    content: List[str]
    last_modified: str


class TargetPostResponse(BaseModel):
    """POST /targets (Response)."""

    key: str
    action: str


class TargetDeleteResponse(BaseModel):
    """DELETE /targets (Response)."""

    key: str
    action: str
