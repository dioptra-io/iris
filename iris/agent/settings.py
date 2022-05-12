"""Agents settings."""

import logging
from enum import Enum
from pathlib import Path
from typing import Optional
from uuid import uuid4

from pydantic import root_validator

from iris.commons.settings import CommonSettings


class RateLimitingMethod(str, Enum):
    auto = "auto"
    active = "active"
    sleep = "sleep"
    none = "none"


class AgentSettings(CommonSettings):
    """Agent specific settings."""

    AGENT_CARACAL_EXCLUDE_PATH: Path = Path("statics/excluded_prefixes")
    AGENT_CARACAL_RATE_LIMITING_METHOD: RateLimitingMethod = RateLimitingMethod.auto
    AGENT_CARACAL_SNIFFER_WAIT_TIME: int = 5
    AGENT_CARACAL_LOGGING_LEVEL: int = logging.INFO
    AGENT_CARACAL_INTEGRITY_CHECK: bool = True

    AGENT_UUID: str = str(uuid4())
    AGENT_UUID_FILE: Optional[Path] = None
    AGENT_MAX_PROBING_RATE: int = 1000  # pps
    AGENT_MIN_TTL: int = -1  # A value < 0 will trigger `find_exit_ttl`
    AGENT_MIN_TTL_FIND_TARGET: str = "example.org"
    AGENT_RIPE_ATLAS_KEY: str = ""
    AGENT_TAGS: str = "all"  # comma-separated list of tags

    AGENT_TARGETS_DIR_PATH: Path = Path("iris_data/agent/targets")
    AGENT_RESULTS_DIR_PATH: Path = Path("iris_data/agent/results")

    AGENT_STOPPER_REFRESH: int = 1  # seconds

    @root_validator
    def load_or_save_uuid(cls, values):
        """
        If an AGENT_UUID_FILE is specified:
        - If it doesn't exist, write AGENT_UUID to it
        - If it exists, read AGENT_UUID from it
        """
        if uuid_file := values.get("AGENT_UUID_FILE"):
            if uuid_file.exists():
                values["AGENT_UUID"] = uuid_file.read_text().strip()
            else:
                uuid_file.write_text(values["AGENT_UUID"])
        return values
