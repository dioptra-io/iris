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

    aws = [
        log_stream(proc.stdout, handler=stdout, prefix=prefix),
        log_stream(proc.stderr, handler=stderr, prefix=prefix),
    ]
    if stopper:
        aws.append(stopper)
    if stdin:
        aws.append(write_stream(proc.stdin, handler=stdin))

    done, pending = await asyncio.wait(aws, return_when=asyncio.FIRST_EXCEPTION)
    for task in pending:
        task.cancel()
    for task in done:
        if task.exception():
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
        await stream.write(data)
    await stream.close()
