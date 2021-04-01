import asyncio
import os
import signal


class CancelProcessException(Exception):
    pass


async def start_stream_subprocess(
    cmd, stdout, stderr, stdin=None, stopper=None, prefix=""
):
    log_prefix = "start_stream_subprocess:"

    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stream_aws = [
        log_stream(proc.stdout, handler=stdout, prefix=prefix),
        log_stream(proc.stderr, handler=stderr, prefix=prefix),
    ]

    if stdin:
        stream_aws.append(write_stream(proc.stdin, handler=stdin))

    aws = [asyncio.gather(*stream_aws)]

    if stopper:
        aws.append(asyncio.create_task(stopper))

    # This will return when either stopper raises an exception, or all the stream
    # handlers have terminated.
    done, pending = await asyncio.wait(aws, return_when=asyncio.FIRST_COMPLETED)
    was_cancelled = False

    for task in pending:
        task.cancel()

    for task in done:
        if task.exception():
            if isinstance(task.exception(), (BrokenPipeError, ConnectionResetError)):
                print(
                    f"{log_prefix} exception: process exited before reading all input"
                )
            elif isinstance(task.exception(), CancelProcessException):
                print(f"{log_prefix} exception: process cancellation requested")
                was_cancelled = True
            else:
                print(f"{log_prefix} exception: {task.exception()}")

    try:
        # NOTE: This triggers a runtime exception in del if the loop is already closed
        os.kill(proc.pid, signal.SIGTERM)
        # The command below seems to also kill the agent...
        # os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        print(f"{log_prefix} cleanup: the process was already terminated")
    except Exception as e:
        print(f"{log_prefix} cleanup: unable to terminate the subprocess: {e}")

    return not was_cancelled


async def log_stream(stream, handler, prefix):
    while True:
        line = await stream.readline()
        if line:
            handler(prefix + line.decode("utf-8").rstrip("\n"))
        else:
            break


async def write_stream(stream, handler):
    buffer = []
    async for data in handler:
        if isinstance(data, str):
            data = data.encode("utf-8") + b"\n"
        buffer.append(data)
        if len(buffer) >= 64:
            stream.writelines(buffer)
            await stream.drain()
            buffer.clear()
    # Flush the remaining entries in the buffer
    stream.writelines(buffer)
    await stream.drain()
    # NOTE: We should not close the stream here, since the consuming process
    # may not have read all of the data yet. Instead we expect the consuming
    # process to exit gracefully when it reaches EOF.
    stream.write_eof()
