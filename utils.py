import io
import zstandard as zstd
from pathlib import Path


def read_lines_from_zst_file(zstd_file_path:Path):
    dctx = zstd.ZstdDecompressor(max_window_size=2**31)
    
    with (
        zstd.open(zstd_file_path, mode='rb', dctx=dctx) as zfh,
        io.TextIOWrapper(zfh) as iofh
    ):
        for line in iofh:
            yield line


