import logging
import logging_loki

from diamond_miner.agent.settings import AgentSettings
from multiprocessing import Queue

settings = AgentSettings()


logger = logging.getLogger("agent")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: AGENT :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

loki_handler = logging_loki.LokiQueueHandler(
    Queue(-1), url=settings.LOKI_URL, version=settings.LOKI_VERSION
)
logger.addHandler(loki_handler)

logger.propagate = False
