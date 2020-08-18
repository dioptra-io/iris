"""Test of common subprocess functions."""

import pytest

from iris.commons.subprocess import start_stream_subprocess


@pytest.mark.asyncio
async def test_subprocess():
    """Test of start_stream_subprocess function."""

    def fake_handler(*args, **kwargs):
        pass

    assert (
        await start_stream_subprocess("echo toto", fake_handler, fake_handler) is True
    )
