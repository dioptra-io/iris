from typing import Dict, Iterable, List, Optional, Tuple

from diamond_miner import mappers
from pytricia import PyTricia

from iris.commons.schemas.public import Round, Tool, ToolParameters


def build_probe_generator_parameters(
    agent_min_ttl: int,
    round_: Round,
    tool: Tool,
    tool_parameters: ToolParameters,
    target_list: Iterable[str],
    prefix_list: Optional[Iterable[str]],
) -> Dict:
    """
    Target list format: `prefix,protocol,min_ttl,max_ttl`
    Prefix list format: `prefix`
    For both lists, `prefix` can be:
        * a network: 8.8.8.0/24, 2001:4860:4860::/64
        * an address: 8.8.8.8, 2001:4860:4860::8888
    Addresses are interpreted as /32 or /128 networks.
    """
    # 1. Instantiate the flow mappers
    flow_mapper_cls = getattr(mappers, tool_parameters.flow_mapper)
    flow_mapper_kwargs = tool_parameters.flow_mapper_kwargs or {}
    flow_mapper_v4 = flow_mapper_cls(
        **{"prefix_size": tool_parameters.prefix_size_v4, **flow_mapper_kwargs}
    )
    flow_mapper_v6 = flow_mapper_cls(
        **{"prefix_size": tool_parameters.prefix_size_v6, **flow_mapper_kwargs}
    )

    prefixes: List[Tuple[str, str, Iterable[int]]] = []
    if tool in [Tool.DiamondMiner, Tool.Yarrp]:
        # 2. Build a radix tree that maps prefix -> [(min_ttl...max_ttl), ...]
        targets: PyTricia[str, List[Tuple[str, range]]] = PyTricia(128)
        for line in target_list:
            prefix, protocol, min_ttl, max_ttl = line.split(",")
            ttls = range(
                # Ensure that the prefix minimum TTL is superior to:
                # - the agent minimum TTL
                # - the round minimum TTL
                max(agent_min_ttl, int(min_ttl), round_.min_ttl),
                # Ensure that the prefix maximum TTL
                # is inferior to the round maximum TTL
                min(int(max_ttl), round_.max_ttl) + 1,
            )
            if todo := targets.get(prefix):
                todo.append((protocol, ttls))
            else:
                targets[prefix] = [(protocol, ttls)]

        # 3. If a specific list of prefixes to probe is specified,
        # generate a new list of prefixes that includes
        # the TTL ranges previously loaded.
        if prefix_list is not None:
            for line in prefix_list:
                prefix = line.strip()
                todo = targets[prefix]
                for protocol, ttls in todo:
                    prefixes.append((prefix, protocol, ttls))
        else:
            # There is no prefix list to probe so we directly take the target list
            for prefix in targets:
                for protocol, ttls in targets[prefix]:
                    prefixes.append((prefix, protocol, ttls))

    elif tool == Tool.Ping:
        # Only take the max TTL in the TTL range
        for line in target_list:
            prefix, protocol, min_ttl, max_ttl = line.split(",")
            prefixes.append((prefix, protocol, (int(max_ttl),)))

    return {
        "prefixes": prefixes,
        "prefix_len_v4": tool_parameters.prefix_len_v4,
        "prefix_len_v6": tool_parameters.prefix_len_v6,
        "flow_ids": range(tool_parameters.n_initial_flows),
        "probe_dst_port": tool_parameters.destination_port,
        "mapper_v4": flow_mapper_v4,
        "mapper_v6": flow_mapper_v6,
    }
