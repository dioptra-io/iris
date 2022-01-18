from iris.commons.models import Tool
from iris.worker.inner_pipeline.diamond_miner import diamond_miner_inner_pipeline
from iris.worker.inner_pipeline.ping import ping_inner_pipeline
from iris.worker.inner_pipeline.probes import probes_inner_pipeline
from iris.worker.inner_pipeline.yarrp import yarrp_inner_pipeline

inner_pipeline_for_tool = {
    Tool.DiamondMiner: diamond_miner_inner_pipeline,
    Tool.Ping: ping_inner_pipeline,
    Tool.Probes: probes_inner_pipeline,
    Tool.Yarrp: yarrp_inner_pipeline,
}
