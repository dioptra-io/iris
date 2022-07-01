import pytest

from iris.agent.ttl import find_exit_ttl_from_output, find_exit_ttl_with_mtr
from tests.helpers import superuser

output = """
Mtr_Version,Start_Time,Status,Host,Hop,Ip,Asn,Loss%,Snt, ,Last,Avg,Best,Wrst,StDev,
MTR.0.94,1640866550,OK,8.8.8.8,1,192.168.1.1,AS???,0.00,1,0,12.22,12.22,12.22,12.22,0.00
MTR.0.94,1640866550,OK,8.8.8.8,2,77.154.43.45,AS15557,0.00,1,0,10.85,10.85,10.85,10.85,0.00
MTR.0.94,1640866550,OK,8.8.8.8,3,77.154.172.1,AS15557,0.00,1,0,9.60,9.60,9.60,9.60,0.00
MTR.0.94,1640866550,OK,8.8.8.8,4,194.6.144.186,AS???,0.00,1,0,29.45,29.45,29.45,29.45,0.00
MTR.0.94,1640866550,OK,8.8.8.8,5,194.6.144.186,AS???,0.00,1,0,26.91,26.91,26.91,26.91,0.00
MTR.0.94,1640866550,OK,8.8.8.8,6,72.14.194.30,AS15169,0.00,1,0,25.58,25.58,25.58,25.58,0.00
MTR.0.94,1640866550,OK,8.8.8.8,7,172.253.69.49,AS15169,0.00,1,0,26.37,26.37,26.37,26.37,0.00
MTR.0.94,1640866550,OK,8.8.8.8,8,108.170.232.125,AS15169,0.00,1,0,25.45,25.45,25.45,25.45,0.00
MTR.0.94,1640866550,OK,8.8.8.8,9,8.8.8.8,AS15169,0.00,1,0,24.41,24.41,24.41,24.41,0.00
""".strip()


def test_find_exit_ttl_from_output():
    assert find_exit_ttl_from_output(output, 2) == 6


def test_find_exit_ttl_from_output_min_ttl():
    assert find_exit_ttl_from_output(output, 7) == 7


def test_find_exit_ttl_from_output_excluded():
    assert find_exit_ttl_from_output(output, 0, excluded=["AS15169"]) == 2


def test_find_exit_ttl_from_output_empty():
    assert find_exit_ttl_from_output("", 2) is None


@superuser
@pytest.mark.cifail
def test_find_exit_ttl_with_mtr():
    assert 2 <= find_exit_ttl_with_mtr("8.8.8.8", 2) <= 255
