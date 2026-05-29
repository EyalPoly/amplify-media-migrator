import queue
import threading
from typing import Optional, cast

_SENTINEL = object()
_PUT_POLL_SECONDS = 0.2


class _StreamCancelled(Exception):
    """Raised inside the download thread's write() once the stream is cancelled."""


class _QueueStream:
    """
    Thread-safe one-way pipe connecting a download thread to s3.upload_fileobj.

    Write side (download thread): write() for each chunk, close_write() on finish or error.
    Read side (upload thread): pass instance directly to s3.upload_fileobj().
    Backpressure: bounded queue blocks the download thread when upload is slower.

    If the reader (upload) abandons the stream — e.g. the S3 upload errors mid-flight —
    cancel() unblocks the download thread so it stops downloading and releases its
    Drive connection instead of blocking forever on a full queue.
    """

    def __init__(self, maxsize: int = 4) -> None:
        self._queue: "queue.Queue[object]" = queue.Queue(maxsize=maxsize)
        self._leftover: bytes = b""
        self._error: Optional[BaseException] = None
        self._bytes_written: int = 0
        self._cancelled = threading.Event()

    # --- write side (download thread) ---

    def write(self, data: bytes) -> int:
        n = len(data)
        while not self._cancelled.is_set():
            try:
                self._queue.put(data, timeout=_PUT_POLL_SECONDS)
            except queue.Full:
                continue
            self._bytes_written += n
            return n
        raise _StreamCancelled()

    def tell(self) -> int:
        return self._bytes_written

    def close_write(self, exc: Optional[BaseException] = None) -> None:
        # Assigning _error before putting the sentinel is load-bearing: the queue's
        # internal lock establishes the happens-before edge so the reader observes
        # the error after it dequeues the sentinel in _drain_next.
        if exc is not None:
            self._error = exc
        if self._cancelled.is_set():
            return
        self._queue.put(_SENTINEL)

    def cancel(self) -> None:
        self._cancelled.set()
        try:
            while True:
                self._queue.get_nowait()
        except queue.Empty:
            pass

    # --- read side (upload thread, called by boto3's upload_fileobj) ---

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            return self._read_all()
        return self._read_up_to(size)

    def _drain_next(self) -> bool:
        """Block until data arrives. Returns False on clean EOF, raises on error."""
        item = self._queue.get()
        if item is _SENTINEL:
            if self._error:
                raise self._error
            return False
        self._leftover = cast(bytes, item)
        return True

    def _read_up_to(self, size: int) -> bytes:
        while not self._leftover:
            if not self._drain_next():
                return b""
        n = min(size, len(self._leftover))
        data = self._leftover[:n]
        self._leftover = self._leftover[n:]
        return data

    def _read_all(self) -> bytes:
        chunks = []
        while True:
            chunk = self._read_up_to(65536)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
