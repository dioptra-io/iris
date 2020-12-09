import logging
import logging_loki

from iris.agent.settings import AgentSettings
from multiprocessing import Queue
from uuid import uuid4

settings = AgentSettings()

AGENT_UUID = str(uuid4()) if settings.AGENT_UUID is None else settings.AGENT_UUID

logger = logging.getLogger("agent")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: AGENT :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

loki_handler = logging_loki.LokiQueueHandler(
    Queue(settings.LOKI_QUEUE_SIZE),
    url=settings.LOKI_URL,
    version=settings.LOKI_VERSION,
    tags={"agent_uuid": AGENT_UUID},
)
loki_handler.setLevel(logging.INFO)
logger.addHandler(loki_handler)

logger.propagate = False
