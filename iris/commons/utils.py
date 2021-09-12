import socket
from ipaddress import IPv4Address, IPv6Address


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
