import secrets
from uuid import uuid4

from iris.commons.filesplit import split_compressed_file
from iris.commons.test import compress_file


def test_split_compressed_file(tmp_path):
    file = tmp_path / str(uuid4())
    compressed_file = file.with_suffix(".csv.zst")
    expected = "\n".join(secrets.token_hex(128) for _ in range(1000))
    file.write_text(expected)
    compress_file(file, compressed_file)
    split_compressed_file(compressed_file, tmp_path / "split_", lines_per_file=100)
    actual = ""
    for file in sorted(tmp_path.glob("split_*")):
        actual += file.read_text()
    assert actual == expected


def test_split_compressed_file_skip_lines(tmp_path):
    file = tmp_path / str(uuid4())
    compressed_file = file.with_suffix(".csv.zst")
    expected = "\n".join(secrets.token_hex(128) for _ in range(1000))
    file.write_text(expected)
    compress_file(file, compressed_file)
    split_compressed_file(
        compressed_file, tmp_path / "split_", lines_per_file=100, skip_lines=10
    )
    actual = ""
    for file in sorted(tmp_path.glob("split_*")):
        actual += file.read_text()
    assert actual == expected[(256 + 1) * 10 :]
