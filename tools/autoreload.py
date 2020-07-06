"""Quick and dirty script to autoreload agent when files are changed."""
import importlib
import logging
import os
import sys
import watchgod

from multiprocessing import Process


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s :: %(levelname)s :: AUTORELOAD :: %(message)s"
)
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def reload_package(path):
    """Quick and dirty module to reload core package."""
    submodules = os.listdir(path)
    for module_str in submodules:
        module_str = module_str.split(".")[0]
        if module_str == "__init__":
            continue
        module = importlib.import_module(path.replace("/", ".") + "." + module_str)
        importlib.reload(module)


def import_from_string(import_str):
    """Get the instance from input string."""

    if not isinstance(import_str, str):
        return import_str

    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        raise ValueError(
            f'Import string "{import_str}" must be in format "<module>:<attribute>".'
        )

    try:
        reload_package("iris/agent")
        module = importlib.import_module(module_str)
        module = importlib.reload(module)
    except ImportError as exc:
        if exc.name != module_str:
            raise exc from None
        raise ValueError(f'Could not import module "{module_str}".')

    instance = module
    try:
        for attr_str in attrs_str.split("."):
            instance = getattr(instance, attr_str)
    except AttributeError:
        raise ValueError(f'Attribute "{attrs_str}" not found in module "{module_str}".')

    return instance


def process_start(app_str):
    """Start a new process."""
    instance = import_from_string(app_str)
    p = Process(target=instance)
    p.start()
    return p


def process_stop(p):
    """Terminate a process."""
    p.terminate()
    p.join()


if __name__ == "__main__":
    try:
        app_str = sys.argv[1]
    except IndexError:
        raise ValueError("Please provide app in parameter")

    logger.info("Starting application process")
    p = process_start(app_str)
    try:
        for changes in watchgod.watch("iris/agent"):
            logger.info("Restarting application process")
            process_stop(p)
            p = process_start(app_str)
    except KeyboardInterrupt:
        process_stop(p)
