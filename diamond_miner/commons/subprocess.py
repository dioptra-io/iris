import asyncio


async def start_stream_subprocess(cmd, logger=None):
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )

    if logger:
        stdout_handler, stderr_handler = logger.info, logger.error
    else:
        stdout_handler, stderr_handler = print, print

    try:
        await asyncio.wait(
            [
                log_stream(proc.stdout, handler=stdout_handler),
                log_stream(proc.stderr, handler=stderr_handler),
            ]
        )
    except Exception:
        proc.terminate()


async def log_stream(stream, handler):
    while True:
        line = await stream.readline()
        if line:
            handler(line.decode("utf-8").rstrip("\n"))
        else:
            break
