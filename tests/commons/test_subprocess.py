from contextlib import redirect_stdout
from io import StringIO

import pytest

from iris.commons.subprocess import start_stream_subprocess


@pytest.mark.asyncio
async def test_subprocess():
    """Test of start_stream_subprocess function."""

    def count_handler():
        def handler(*args, **kwargs):
            handler.calls += 1

        handler.calls = 0
        return handler

    def crash_handler():
        def handler(*args, **kwargs):
            raise Exception("Simulated exception")

        return handler

    # Base case: we call a standalone program and check if we get the full output.
    stdout = count_handler()
    stderr = count_handler()
    with redirect_stdout(StringIO()):
        await start_stream_subprocess("yes | head -n 100", stdout=stdout, stderr=stderr)

        assert stdout.calls == 100
        assert stderr.calls == 0

    # Output handler exception: check that start_stream_subprocess does not crash
    # if there is an exception in the handlers.
    with redirect_stdout(StringIO()):
        await start_stream_subprocess(
            "yes | head -n 100", stdout=crash_handler(), stderr=crash_handler()
        )
