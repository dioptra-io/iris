from collections import defaultdict
from typing import Callable, Dict

from iris.commons.models import Tool
from iris.worker.inner_pipeline.diamond_miner import diamond_miner_inner_pipeline
from iris.worker.inner_pipeline.probes import probes_inner_pipeline

inner_pipeline_for_tool: Dict[Tool, Callable] = defaultdict(
    lambda: diamond_miner_inner_pipeline
)
inner_pipeline_for_tool[Tool.Probes] = probes_inner_pipeline
