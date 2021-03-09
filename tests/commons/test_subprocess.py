"""Test of common subprocess functions."""

import asyncio

import pytest

from iris.commons.subprocess import CancelProcessException, start_stream_subprocess


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

    async def arange(count):
        for i in range(count):
            yield (str(i))

    async def arange_crash(count):
        for i in range(count):
            yield (str(i))
        raise Exception("Simulated exception")

    async def stopper(seconds):
        await asyncio.sleep(seconds)
        raise CancelProcessException

    # Base case: we call a standalone program and check if we get the full output.
    stdout = count_handler()
    stderr = count_handler()
    res = await start_stream_subprocess(
        "yes | head -n 100", stdout=stdout, stderr=stderr
    )
    assert stdout.calls == 100
    assert stderr.calls == 0
    assert res is True

    # EOF: check that the consuming process stops when the input iterator is finished.
    stdout = count_handler()
    stderr = count_handler()
    res = await start_stream_subprocess(
        "head -n 5", stdout=stdout, stderr=stderr, stdin=arange(2)
    )
    assert stdout.calls == 2
    assert stderr.calls == 0
    assert res is True

    # Early exit: check that start_stream_subprocess does not crash if the subprocess
    # terminates without having consumed the full input.
    # NOTE: This is error appears randomly... so let's check it many times.
    for _ in range(10):
        stdout = count_handler()
        stderr = count_handler()
        res = await start_stream_subprocess(
            "head -n 5", stdout=stdout, stderr=stderr, stdin=arange(10)
        )
        assert stdout.calls == 5
        assert stderr.calls == 0

    # Output handler exception: check that start_stream_subprocess does not crash
    # if there is an exception in the handlers.
    res = await start_stream_subprocess(
        "yes | head -n 100", stdout=crash_handler(), stderr=crash_handler()
    )
    assert res is True

    # Input handler exception
    res = await start_stream_subprocess(
        "head -n 100",
        stdout=count_handler(),
        stderr=count_handler(),
        stdin=arange_crash(100),
    )
    assert res is True

    # Cancel exception: check that the subprocess is cancelled when requested.
    stdout = count_handler()
    stderr = count_handler()
    res = await start_stream_subprocess(
        "echo toto && sleep 1 && echo toto",
        stdout=stdout,
        stderr=stderr,
        stopper=stopper(0.5),
    )
    assert stdout.calls == 1
    assert stderr.calls == 0
    assert res is False
