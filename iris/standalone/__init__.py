from enum import Enum

from iris.api.schemas import ToolParameters


class Tool(str, Enum):
    diamond_miner = "diamond-miner"
    diamond_miner_ping = "ping"


default_parameters = ToolParameters()
