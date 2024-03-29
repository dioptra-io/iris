import socket
from ipaddress import IPv4Address, IPv6Address

from iris.commons.utils import (
    get_internal_ipv4_address,
    get_internal_ipv6_address,
    zstd_stream_reader,
    zstd_stream_reader_text,
    zstd_stream_writer,
)


class MockSocket:
    def __init__(self, *args, **kwargs):
        pass

    def connect(self, *args, **kwargs):
        pass

    def getsockname(self, *args, **kwargs):
        pass

    def close(self):
        pass


def test_get_ipv4_address(monkeypatch):
    """Test get own ipv4 address."""

    class Socket(MockSocket):
        def getsockname(self, *args, **kwargs):
            return ("1.2.3.4",)

    monkeypatch.setattr(socket, "socket", Socket)
    assert get_internal_ipv4_address() == IPv4Address("1.2.3.4")


def test_get_ipv4_address_error(monkeypatch):
    """Test get own ipv4 address when it's not possible."""

    class Socket(MockSocket):
        def getsockname(self, *args, **kwargs):
            raise Exception

    monkeypatch.setattr(socket, "socket", Socket)
    assert not get_internal_ipv4_address()


def test_get_ipv6_address(monkeypatch):
    """Test get own ipv6 address."""

    class Socket(MockSocket):
        def getsockname(self, *args, **kwargs):
            return ("::1234",)

    monkeypatch.setattr(socket, "socket", Socket)
    assert get_internal_ipv6_address() == IPv6Address("::1234")


def test_get_ipv6_address_error(monkeypatch):
    """Test get own ipv6 address when it's not possible."""

    class Socket(MockSocket):
        def getsockname(self, *args, **kwargs):
            raise Exception

    monkeypatch.setattr(socket, "socket", Socket)
    assert not get_internal_ipv6_address()


def test_zstd_stream(tmp_path):
    file = tmp_path / "test.zst"
    with zstd_stream_writer(file) as f:
        f.write(b"Hello\nWorld")
    with zstd_stream_reader(file) as f:
        assert f.readall() == b"Hello\nWorld"
    with zstd_stream_reader_text(file) as f:
        assert list(f) == ["Hello\n", "World"]
