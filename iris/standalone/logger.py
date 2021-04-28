import logging


def create_logger(level):
    formatter = logging.Formatter("%(asctime)s :: %(levelname)s :: %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    stream_handler.setFormatter(formatter)
    logger = logging.getLogger("standalone")
    logger.setLevel(level)
    logger.addHandler(stream_handler)

    # Caracal logger
    logger_ca = logging.getLogger("caracal")
    logger_ca.setLevel(logging.DEBUG)
    logger_ca.addHandler(stream_handler)

    return logger
