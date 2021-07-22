from typing import List, Optional

from pydantic import BaseModel


class Reply(BaseModel):
    """Probe reply information (Response)."""

    probe_protocol: int
    probe_src_addr: str
    probe_dst_addr: str
    probe_src_port: int
    probe_dst_port: int
    probe_ttl: int
    quoted_ttl: int
    reply_src_addr: str
    reply_protocol: int
    reply_icmp_type: int
    reply_icmp_code: int
    reply_ttl: int
    reply_size: int
    reply_mpls_labels: List[int]
    rtt: float
    round: int


class Interface(BaseModel):
    """Probe reply information (Response)."""

    ttl: int
    addr: str


class Link(BaseModel):
    """Probe reply information (Response)."""

    near_ttl: int
    near_addr: str
    far_addr: str


class Prefix(BaseModel):
    """Probe reply information (Response)."""

    prefix: str
    has_amplification: bool
    has_loops: bool


class Replies(BaseModel):
    """GET /results/{measurement_uuid}/{agent_uuid}/replies/{prefix} (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[Reply]


class Interfaces(BaseModel):
    """GET /results/{measurement_uuid}/{agent_uuid}/interfaces/{prefix} (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[Interface]


class Links(BaseModel):
    """GET /results/{measurement_uuid}/{agent_uuid}/links/{prefix} (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[Link]


class Prefixes(BaseModel):
    """GET /results/{measurement_uuid}/{agent_uuid}/prefixes (Response)."""

    count: int
    next: Optional[str] = None
    previous: Optional[str] = None
    results: List[Prefix]
