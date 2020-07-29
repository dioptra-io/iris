import asyncio


async def start_stream_subprocess(cmd, stdout, stderr, prefix=""):
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )

    try:
        await asyncio.wait(
            [
                log_stream(proc.stdout, handler=stdout, prefix=prefix),
                log_stream(proc.stderr, handler=stderr, prefix=prefix),
            ]
        )
    except Exception:
        proc.terminate()


async def log_stream(stream, handler, prefix):
    while True:
        line = await stream.readline()
        if line:
            handler(prefix + line.decode("utf-8").rstrip("\n"))
        else:
            break
