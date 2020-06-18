import logging

logger = logging.getLogger("agent")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: AGENT :: %(message)s")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.propagate = False
