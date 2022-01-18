import pytest
from pydantic import ValidationError

from iris.commons.models import MeasurementAgentCreate


def test_create_missing_tag_uuid():
    with pytest.raises(ValidationError, match="one of `uuid` or `tag`"):
        MeasurementAgentCreate()


def test_create_tag_and_uuid():
    with pytest.raises(ValidationError, match="one of `uuid` or `tag`"):
        MeasurementAgentCreate(tag="tag", uuid="uuid")
