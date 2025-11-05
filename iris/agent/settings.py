"""Agents settings."""

import logging
from enum import Enum
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import model_validator

from iris.commons.settings import CommonSettings


class AgentSettings(CommonSettings):
    """Agent specific settings."""

    AGENT_BACKEND: Literal["atlas", "caracal"] = "caracal"

    AGENT_CARACAL_EXCLUDE_PATH: Path = Path("statics/excluded_prefixes")
    AGENT_CARACAL_INTEGRITY_CHECK: bool = True
    AGENT_CARACAL_SNIFFER_WAIT_TIME: int = 5
    
    AGENT_UUID: str = str(uuid4())
    AGENT_UUID_FILE: Path | None = None
    AGENT_MAX_PROBING_RATE: int = 1000  # pps
    AGENT_MIN_TTL: int = -1  # A value < 0 will trigger `find_exit_ttl`
    AGENT_MIN_TTL_FIND_TARGET: str = "example.org"
    AGENT_RIPE_ATLAS_KEY: str = ""
    AGENT_TAGS: str = "all"  # comma-separated list of tags

    AGENT_TARGETS_DIR_PATH: Path = Path("iris_data/agent/targets")
    AGENT_RESULTS_DIR_PATH: Path = Path("iris_data/agent/results")

    AGENT_STOPPER_REFRESH: int = 1  # seconds

    @model_validator(mode='after')
    def load_or_save_uuid(self):
        """
        If an AGENT_UUID_FILE is specified:
        - If it doesn't exist, write AGENT_UUID to it
        - If it exists, read AGENT_UUID from it
        """
        uuid_file = self.AGENT_UUID_FILE
        if uuid_file:
            if uuid_file.exists():
                new_uuid = uuid_file.read_text().strip()
                return self.model_copy(update={'AGENT_UUID': new_uuid})
            else:
                uuid_file.write_text(self.AGENT_UUID)
        return self
