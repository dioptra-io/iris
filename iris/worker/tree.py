from typing import Iterable, List, Tuple

from pytricia import PyTricia


def load_targets(
    target_list: Iterable[str], clamp_ttl_min=0, clamp_ttl_max=255
) -> PyTricia:
    """
    Returns a radix-tree that maps prefixes to a list of (protocol, ttls, n_initial_flows).

    Target list format: `prefix,protocol,min_ttl,max_ttl,n_initial_flows`
    `prefix` can be:
        * a network: 8.8.8.0/24, 2001:4860:4860::/64
        * an address: 8.8.8.8, 2001:4860:4860::8888
    Addresses are interpreted as /32 or /128 networks.

    >>> tree = load_targets(["8.8.8.0/24,1,11,12,6", "8.8.8.0/24,1,14,20,6"], clamp_ttl_min=11, clamp_ttl_max=16)
    >>> tree["8.8.8.1"]
    [('1', range(11, 13), 6), ('1', range(14, 17), 6)]
    >>> tree["8.8.4.0"]
    Traceback (most recent call last):
        ...
    KeyError: 'Prefix not found.'
    """
    tree: PyTricia[str, List[Tuple[str, range, int]]] = PyTricia(128)
    for line in target_list:
        prefix, protocol, min_ttl, max_ttl, n_initial_flows = line.split(",")
        ttls = range(
            max(clamp_ttl_min, int(min_ttl)),
            min(clamp_ttl_max, int(max_ttl)) + 1,
        )
        if todo := tree.get(prefix):
            todo.append((protocol, ttls, int(n_initial_flows)))
        else:
            tree[prefix] = [(protocol, ttls, int(n_initial_flows))]
    return tree
