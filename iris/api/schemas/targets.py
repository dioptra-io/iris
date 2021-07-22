from typing import List, Optional

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


class Targets(BaseModel):
    """GET /targets (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[TargetSummary]


class TargetPostResponse(BaseModel):
    """POST /targets (Response)."""

    key: str
    action: str


class TargetDeleteResponse(BaseModel):
    """DELETE /targets (Response)."""

    key: str
    action: str
