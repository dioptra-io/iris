import logging

logger = logging.getLogger("agent")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: AGENT :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

__version__ = "0.1.0"
