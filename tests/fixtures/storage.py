from uuid import uuid4

import pytest


@pytest.fixture
def make_bucket():
    def _make_bucket():
        return f"iris-test-{uuid4()}"

    return _make_bucket


@pytest.fixture
def make_tmp_file(tmp_path):
    def _make_tmp_file(name=None):
        name = name or str(uuid4())
        content = str(uuid4())
        metadata = {"meta": str(uuid4())}
        tmp_file = tmp_path / name
        tmp_file.write_text(content)
        return dict(
            content=content, metadata=metadata, name=tmp_file.name, path=tmp_file
        )

    return _make_tmp_file
