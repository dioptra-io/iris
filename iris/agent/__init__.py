import logging
import logging_loki

from iris.agent.settings import AgentSettings
from multiprocessing import Queue

settings = AgentSettings()


logger = logging.getLogger("agent")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: AGENT :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

loki_handler = logging_loki.LokiQueueHandler(
    Queue(-1), url=settings.LOKI_URL, version=settings.LOKI_VERSION
)
loki_handler.setLevel(logging.INFO)
logger.addHandler(loki_handler)

logger.propagate = False
