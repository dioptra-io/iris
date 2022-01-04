from collections import defaultdict
from typing import Callable, Dict

from iris.commons.models.diamond_miner import Tool
from iris.worker.inner_pipeline.default import default_inner_pipeline
from iris.worker.inner_pipeline.probes import probes_inner_pipeline

inner_pipeline_for_tool: Dict[Tool, Callable] = defaultdict(
    lambda: default_inner_pipeline
)
inner_pipeline_for_tool[Tool.Probes] = probes_inner_pipeline
