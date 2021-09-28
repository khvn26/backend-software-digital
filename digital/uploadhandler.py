import gzip
import io
from typing import TYPE_CHECKING, Optional

from django.core.files.uploadhandler import FileUploadHandler

if TYPE_CHECKING:
    from django.http.request import HttpRequest  # pragma: no cover


class StreamGzipReader(gzip._GzipReader):
    def read_chunk(self, size=-1):
        # size=0 is special because decompress(max_length=0) is not supported
        if not size:
            return b""

        # For certain input data, a single
        # call to decompress() may not return
        # any data. In this case, retry until we get some data or reach EOF.
        while True:
            if self._decompressor.eof:
                # Ending case: we've come to the end of a member in the file,
                # so finish up this member, and read a new gzip header.
                # Check the CRC and file size, and set the flag so we read
                # a new member
                self._read_eof()
                self._new_member = True
                self._decompressor = self._decomp_factory(**self._decomp_args)

            if self._new_member:
                # If the _new_member flag is set, we have to
                # jump to the next member, if there is one.
                self._init_read()
                if not self._read_gzip_header():
                    self._size = self._pos
                    return b""
                self._new_member = False

            # Read a chunk of data from the file
            buf = self._fp.read(io.DEFAULT_BUFFER_SIZE)

            uncompress = self._decompressor.decompress(buf)
            if self._decompressor.unconsumed_tail != b"":
                self._fp.prepend(self._decompressor.unconsumed_tail)
            elif self._decompressor.unused_data != b"":
                # Prepend the already read bytes to the fileobj so they can
                # be seen by _read_eof() and _read_gzip_header()
                self._fp.prepend(self._decompressor.unused_data)

            if uncompress != b"" or buf == b"":
                break

        self._add_read_data(uncompress)
        self._pos += len(uncompress)
        return uncompress


class GZipUploadHandler(FileUploadHandler):
    """
    File upload handler to decompress gzipped content on the fly.
    """

    def __init__(
        self, request: "HttpRequest", header_bytes: Optional[bytes] = None
    ) -> None:
        super().__init__(request=request)
        self.buf = buf = io.BytesIO()
        if header_bytes:
            buf.write(header_bytes)
        self.gzipped = StreamGzipReader(buf)

    def receive_data_chunk(self, raw_data: bytes, start: int) -> bytes:
        self.buf.write(raw_data)
        self.buf.seek(start)
        data = self.gzipped.read_chunk()
        self.buf.truncate()
        return data

    def file_complete(self, *_) -> None:
        self.gzipped.close()
        self.buf.close()
