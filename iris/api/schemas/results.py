from typing import List

from pydantic import BaseModel


class Reply(BaseModel):
    """Probe reply information (Response)."""

    probe_protocol: str
    probe_src_addr: str
    probe_dst_addr: str
    probe_src_port: int
    probe_dst_port: int
    probe_ttl: int
    quoted_ttl: int
    reply_src_addr: str
    reply_protocol: str
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
    far_ttl: int
    near_addr: str
    far_addr: str


class Prefix(BaseModel):
    """Probe reply information (Response)."""

    prefix: str
    has_amplification: bool
    has_loops: bool
