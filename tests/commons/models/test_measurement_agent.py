import pytest
from pydantic import ValidationError
from iris.commons.models.measurement_agent import MeasurementAgentCreate

def test_create_missing_tag_uuid():
    """Fail when both uuid and tag are missing, but required fields are present."""
    with pytest.raises(ValidationError) as exc_info:
        MeasurementAgentCreate(
            target_file="dummy.txt",  # provide all required fields
            # uuid and tag both missing
        )

    errors = exc_info.value.errors()
    # Check for validator error message
    assert any("one of `uuid` or `tag`" in error["msg"] for error in errors)


def test_create_tag_and_uuid():
    """Fail when both uuid and tag are provided simultaneously."""
    with pytest.raises(ValidationError) as exc_info:
        MeasurementAgentCreate(
            target_file="dummy.txt",  # required field
            uuid="123",
            tag="tag"
        )

    errors = exc_info.value.errors()
    assert any("one of `uuid` or `tag`" in error["msg"] for error in errors)

