import asyncio
import json
import socket
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from io import TextIOWrapper
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import IO, TypeVar

from pydantic import BaseModel
from sqlmodel import SQLModel
from zstandard import (
    ZstdCompressionWriter,
    ZstdCompressor,
    ZstdDecompressionReader,
    ZstdDecompressor,
)

T = TypeVar("T")


async def cancel_task(task: asyncio.Task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def cast(to: Callable[..., BaseModel], from_: BaseModel, **extra) -> T:
    """Convert an (SQL)Model to another, including extra fields."""
    data = {}
    for field in from_.__fields__:
        data[field] = getattr(from_, field)
    if isinstance(from_, SQLModel):
        for field in from_.__sqlmodel_relationships__:
            data[field] = getattr(from_, field)
    return to.parse_obj({**data, **extra})  # type: ignore


def unwrap(value: T | None) -> T:
    assert value, "unexpected None value"
    return value


def json_serializer(obj):
    try:
        # Try Pydantic `.json()` first.
        return obj.json()
    except AttributeError:
        return json.dumps(obj)


def get_ipv4_address(host="8.8.8.8", port=80) -> IPv4Address:
    """Find local IPv4 address."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((host, port))
        ip_address = IPv4Address(s.getsockname()[0])
    except Exception:
        ip_address = IPv4Address("127.0.0.1")
    finally:
        s.close()
    return ip_address


def get_ipv6_address(host="2001:4860:4860::8888", port=80) -> IPv6Address:
    """Find local IPv6 address."""
    s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    try:
        s.connect((host, port))
        ip_address = IPv6Address(s.getsockname()[0])
    except Exception:
        ip_address = IPv6Address("::1")
    finally:
        s.close()
    return ip_address


@contextmanager
def zstd_stream_reader(path: Path | str) -> Iterator[ZstdDecompressionReader]:
    ctx = ZstdDecompressor()
    with open(path, "rb") as f, ctx.stream_reader(f) as stream:
        yield stream


@contextmanager
def zstd_stream_reader_text(path: Path | str) -> Iterator[IO[str]]:
    ctx = ZstdDecompressor()
    with open(path, "rb") as f, ctx.stream_reader(f) as stream:
        yield TextIOWrapper(stream)


@contextmanager
def zstd_stream_writer(path: Path | str) -> Iterator[ZstdCompressionWriter]:
    ctx = ZstdCompressor()
    with open(path, "wb") as f, ctx.stream_writer(f) as stream:
        yield stream
