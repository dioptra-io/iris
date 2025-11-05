import pytest
from pydantic import ValidationError

from iris.commons.models import MeasurementAgentCreate

def test_create_missing_tag_uuid():
    """Fail when both uuid and tag are missing."""
    with pytest.raises(ValidationError) as exc_info:
        MeasurementAgentCreate()

    errors = exc_info.value.errors()
    # Check that at least one error message mentions 'one of `uuid` or `tag`'
    assert any("one of `uuid` or `tag`" in error["msg"] for error in errors)


def test_create_tag_and_uuid():
    """Fail when both uuid and tag are provided simultaneously."""
    with pytest.raises(ValidationError) as exc_info:
        MeasurementAgentCreate(tag="tag", uuid="uuid")

    errors = exc_info.value.errors()
    assert any("one of `uuid` or `tag`" in error["msg"] for error in errors)

