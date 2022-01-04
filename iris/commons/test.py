from pydantic import BaseModel
from zstandard import ZstdCompressor, ZstdDecompressor


class TestModel(BaseModel):
    a: int


def compress_file(input_path, output_path):
    with open(input_path, "rb") as inp:
        with open(output_path, "wb") as out:
            ctx = ZstdCompressor()
            ctx.copy_stream(inp, out)


def decompress_file(input_path, output_path):
    with open(input_path, "rb") as inp:
        with open(output_path, "wb") as out:
            ctx = ZstdDecompressor()
            ctx.copy_stream(inp, out)
