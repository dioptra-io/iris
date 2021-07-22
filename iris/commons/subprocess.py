import asyncio


async def start_stream_subprocess(cmd, stdout, stderr, prefix="", **kwargs):
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, **kwargs
    )

    stream_aws = [
        log_stream(proc.stdout, handler=stdout, prefix=prefix),
        log_stream(proc.stderr, handler=stderr, prefix=prefix),
    ]

    await asyncio.gather(*stream_aws)


async def log_stream(stream, handler, prefix):
    while True:
        line = await stream.readline()
        if line:
            try:
                handler(prefix + line.decode("utf-8").rstrip("\n"))
            except Exception as e:
                print(e)
        else:
            break
