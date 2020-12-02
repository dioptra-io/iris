import asyncio
import os
import signal


async def start_stream_subprocess(
    cmd, stdout, stderr, stdin=None, stopper=None, prefix=""
):
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
        aws.append(stopper)

    # This will return when either stopper raises an exception, or all the stream
    # handlers have terminated.
    done, pending = await asyncio.wait(aws, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    for task in done:
        if task.exception():
            print("start_stream_subprocess exception: ", task.exception())
            # TODO: Check if the process is still running before killing it?
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            return False
    return True


async def log_stream(stream, handler, prefix):
    while True:
        line = await stream.readline()
        if line:
            handler(prefix + line.decode("utf-8").rstrip("\n"))
        else:
            break


async def write_stream(stream, handler):
    async for data in handler:
        if isinstance(data, str):
            data = data.encode("utf-8") + b"\n"
        stream.write(data)
        await stream.drain()
    # NOTE: We should not close the stream here, since the consuming process
    # may not have read all of the data yet. Instead we expect the consuming
    # process to exit gracefully when it reaches EOF.
    stream.write_eof()
