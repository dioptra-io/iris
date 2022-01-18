from pathlib import Path

from pydantic import BaseModel
from zstandard import ZstdCompressor, ZstdDecompressor


class TestModel(BaseModel):
    a: int


def compress_file(input_path, output_path=None):
    if not output_path:
        output_path = str(input_path) + ".zst"
    with open(input_path, "rb") as inp:
        with open(output_path, "wb") as out:
            ctx = ZstdCompressor()
            ctx.copy_stream(inp, out)
    return Path(output_path)


def decompress_file(input_path, output_path=None):
    if not output_path:
        output_path = str(input_path).replace(".zst", "")
    with open(input_path, "rb") as inp:
        with open(output_path, "wb") as out:
            ctx = ZstdDecompressor()
            ctx.copy_stream(inp, out)
    return Path(output_path)
