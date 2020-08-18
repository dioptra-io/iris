"""Test of common utils functions."""

import socket

from iris.commons.utils import get_own_ip_address


def test_get_own_ip_address(monkeypatch):
    """Test get own ip address."""

    class Socket(object):
        def __init__(self, *args, **kwargs):
            pass

        def connect(self, *args, **kwargs):
            pass

        def getsockname(self, *args, **kwargs):
            return ("1.2.3.4",)

        def close(self):
            pass

    monkeypatch.setattr(socket, "socket", Socket)
    assert get_own_ip_address() == "1.2.3.4"


def test_get_own_ip_address_error(monkeypatch):
    """Test get own ip address when it's not possible."""

    class Socket(object):
        def __init__(self, *args, **kwargs):
            pass

        def connect(self, *args, **kwargs):
            pass

        def getsockname(self, *args, **kwargs):
            raise Exception

        def close(self):
            pass

    monkeypatch.setattr(socket, "socket", Socket)
    assert get_own_ip_address() == "127.0.0.1"
