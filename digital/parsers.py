import gzip
import struct
from typing import IO, Any, NamedTuple, Optional, Tuple

from rest_framework.parsers import FileUploadParser

from digital.uploadhandler import GZipUploadHandler


class GzInfo(NamedTuple):
    fname: Optional[str]
    method: int
    flag: int
    last_mtime: int


class GzipFileUploadParser(FileUploadParser):
    """
    Detect a gzipped file, decompress and use correct filename when detected.
    """

    def get_filename(
        self, stream: IO[bytes], media_type: str, parser_context: dict[str, Any]
    ) -> Optional[str]:
        filename = super().get_filename(stream, media_type, parser_context)
        # We can't rely on seekable requests, so we have to store the data we need to check for gzip;
        # In case of plaintext tsv, we're only losing a small part of header, which we won't require
        gz_info, read_bytes = _get_gz_info(stream)
        if gz_info:
            # This is a suitable moment to inject the gzipped file upload handler
            request = parser_context["request"]
            request.upload_handlers.insert(
                0,
                GZipUploadHandler(request, read_bytes),
            )
            # Given that we have original filenames that differ from gzipped filenames,
            # we want to prefer the original one
            return gz_info.fname or filename
        return filename


def _get_gz_info(fp: IO[bytes]) -> Tuple[Optional[GzInfo], bytes]:
    # the magic 2 bytes: if 0x1f 0x8b (037 213 in octal)
    magic = fp.read(2)
    read_bytes = bytearray(magic)

    if magic != b"\037\213":
        return None, bytes(read_bytes)

    next_bytes, whole = _read_exact(fp, 8)
    read_bytes += next_bytes
    if not whole:
        return None, bytes(read_bytes)
    (method, flag, last_mtime) = struct.unpack("<BBIxx", next_bytes)
    if method != 8:
        return None, bytes(read_bytes)

    # Case where the name is not in the header according to flag
    if not flag & gzip.FNAME:
        return (
            GzInfo(
                fname=None,
                method=method,
                flag=flag,
                last_mtime=last_mtime,
            ),
            bytes(read_bytes),
        )

    if flag & gzip.FEXTRA:
        # Read & discard the extra field, if present

        next_bytes, whole = _read_exact(fp, 2)
        read_bytes += next_bytes
        if not whole:
            return None, bytes(read_bytes)
        (extra_len,) = struct.unpack("<H", next_bytes)
        next_bytes, whole = _read_exact(fp, extra_len)
        read_bytes += next_bytes
        if not whole:
            return None, bytes(read_bytes)

    _fname = []  # bytes for fname
    if flag & gzip.FNAME:
        # Read a null-terminated string containing the filename
        # RFC 1952 <https://tools.ietf.org/html/rfc1952>
        #    specifies FNAME is encoded in latin1
        while True:
            s = fp.read(1)
            read_bytes += s
            if not s or s == b"\000":
                break
            _fname.append(s)
        return (
            GzInfo(
                fname="".join([s.decode("latin1") for s in _fname]),
                method=method,
                flag=flag,
                last_mtime=last_mtime,
            ),
            bytes(read_bytes),
        )


def _read_exact(fp: IO[bytes], n: int) -> Tuple[bytes, bool]:
    """
    This is the gzip.GzipFile._read_exact() method from the Python stlib.
    """
    data = fp.read(n)
    while len(data) < n:
        b = fp.read(n - len(data))
        if not b:
            return data, False
        data += b
    return data, True
