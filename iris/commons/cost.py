from ipaddress import IPv4Network, ip_network
from typing import Iterable

from iris.commons.models import Tool, ToolParameters

PROBE_SIZE_GB = 100 / 1e9
"""Conservative estimate on the size of an IPv6 probe."""


# TODO: Move this function to diamond-miner.
def count_probes(targets: Iterable[str], prefix_len_v4: int, prefix_len_v6: int) -> int:
    """
    >>> count_probes(["192.0.2.0/24,icmp,2,32,6"], 25, 64)
    372
    """
    count = 0
    for target in targets:
        prefix, protocol, min_ttl, max_ttl, n_initial_flows = target.split(",")
        network = ip_network(prefix)
        if isinstance(network, IPv4Network):
            prefix_len = prefix_len_v4
        else:
            prefix_len = prefix_len_v6
        if network.prefixlen > prefix_len:
            raise ValueError(f"prefix length must be <= {prefix_len}")
        n_prefixes = 2 ** (prefix_len - network.prefixlen)
        count += n_prefixes * (int(max_ttl) - int(min_ttl) + 1) * int(n_initial_flows)
    return count


def estimate_single_round_cost(
    parameters: ToolParameters, targets: Iterable[str]
) -> float:
    """Cost estimate for single round measurements."""
    n_probes = count_probes(
        targets,
        prefix_len_v4=parameters.prefix_len_v4,
        prefix_len_v6=parameters.prefix_len_v6,
    )
    return n_probes * PROBE_SIZE_GB


def estimate_diamond_miner_cost(
    parameters: ToolParameters, targets: Iterable[str]
) -> float:
    """Cost estimate for multi-round diamond-miner measurements."""
    n_probes = count_probes(
        targets,
        prefix_len_v4=parameters.prefix_len_v4,
        prefix_len_v6=parameters.prefix_len_v6,
    )
    return n_probes * PROBE_SIZE_GB * 7


def estimate_probes_cost(parameters: ToolParameters, probes: Iterable[str]) -> float:
    n_probes = sum(1 for _ in probes)
    return n_probes * PROBE_SIZE_GB


estimate_cost_for_tool = {
    Tool.DiamondMiner: estimate_diamond_miner_cost,
    Tool.Ping: estimate_single_round_cost,
    Tool.Probes: estimate_probes_cost,
    Tool.Yarrp: estimate_single_round_cost,
}
