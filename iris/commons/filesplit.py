from collections.abc import Iterable
from io import TextIOWrapper
from math import ceil
from typing import IO

from iris.commons.utils import zstd_stream_reader, zstd_stream_reader_text

DEFAULT_ESTIMATE_MAX_LINES = 1000


def estimate_line_size(f: IO[bytes], *, max_lines: int = DEFAULT_ESTIMATE_MAX_LINES):
    """
    >>> from io import BytesIO
    >>> estimate_line_size(BytesIO(b""))
    0
    >>> estimate_line_size(BytesIO(b"1234"))
    4
    >>> estimate_line_size(BytesIO(b"1234\\n"))
    5
    >>> estimate_line_size(BytesIO(b"1234\\n5678\\n"))
    5
    """
    n_lines = 0
    first_byte = f.tell()
    wrapper = TextIOWrapper(f)
    for _ in wrapper:
        n_lines += 1
        if n_lines >= max_lines:
            break
    bytes_read = f.tell() - first_byte
    return int(ceil(bytes_read / (n_lines or 1)))


def split_stream(
    stream: IO[str], split_boundary: str, split_size: int, *, read_size: int = 2**20
) -> Iterable[str | int]:
    """
    >>> from io import StringIO
    >>> list(split_stream(StringIO("1234\\n5678\\n"), "\\n", 5, read_size=5)) # Aligned read
    [0, '1234\\n', 1, '5678\\n', 2]
    >>> list(split_stream(StringIO("1234\\n5678\\n"), "\\n", 5, read_size=2)) # Smaller unaligned read
    [0, '12', '34', '\\n', 1, '5', '67', '8\\n', 2]
    >>> list(split_stream(StringIO("1234\\n5678\\n"), "\\n", 5, read_size=8)) # Larger unaligned read
    [0, '1234\\n', 1, '567', '8\\n', 2]
    >>> list(split_stream(StringIO("1234\\n5678\\n"), "\\n", 2, read_size=3)) # Split and read size smaller than line size
    [0, '123', '4\\n', 1, '5', '678', '\\n', 2]
    """
    bytes_read = 0
    split_index = 0
    yield split_index
    while True:
        chunk = stream.read(read_size)
        bytes_read += read_size
        if not chunk:
            break
        if bytes_read < split_size:
            yield chunk
        else:
            # The current split is larger than the desired size.
            s = chunk.split(split_boundary, maxsplit=1)
            if len(s) == 1:
                # Boundary not found, continue to yield chunks on this split.
                yield s[0]
            else:
                # Boundary found, increment the split and yield leftover data.
                yield s[0] + split_boundary
                bytes_read = len(s[1])
                split_index += 1
                yield split_index
                if s[1]:
                    yield s[1]


def split_compressed_file(
    input_file: str,
    output_prefix: str,
    lines_per_file: int,
    *,
    max_estimate_lines: int = DEFAULT_ESTIMATE_MAX_LINES,
    skip_lines: int = 0,
):
    with zstd_stream_reader(input_file) as f:
        line_size = estimate_line_size(f, max_lines=max_estimate_lines)
    split_size = lines_per_file * line_size
    with zstd_stream_reader_text(input_file) as f:
        outf = None
        for _ in range(skip_lines):
            next(f)
        for chunk in split_stream(f, "\n", split_size):
            if isinstance(chunk, int):
                if outf:
                    outf.close()
                outf = open(f"{output_prefix}_{chunk}", "w")
            else:
                outf.write(chunk)  # type: ignore
        if outf:
            outf.close()
