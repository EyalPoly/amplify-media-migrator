import threading
import time

import pytest

from amplify_media_migrator.utils.stream import _QueueStream

pytestmark = pytest.mark.unit


class TestQueueStream:
    def test_single_chunk_passes_through(self) -> None:
        s = _QueueStream()
        s.write(b"hello")
        s.close_write()
        assert s.read(5) == b"hello"

    def test_multiple_chunks_concatenate(self) -> None:
        s = _QueueStream()
        s.write(b"foo")
        s.write(b"bar")
        s.close_write()
        assert s.read(3) == b"foo"
        assert s.read(3) == b"bar"
        assert s.read(1) == b""

    def test_clean_eof_returns_empty_bytes(self) -> None:
        s = _QueueStream()
        s.close_write()
        assert s.read(10) == b""

    def test_error_propagates_after_partial_data(self) -> None:
        s = _QueueStream()
        s.write(b"partial")
        s.close_write(ValueError("boom"))
        assert s.read(7) == b"partial"
        with pytest.raises(ValueError, match="boom"):
            s.read(1)

    def test_tell_tracks_bytes_written(self) -> None:
        s = _QueueStream()
        assert s.tell() == 0
        s.write(b"abc")
        assert s.tell() == 3
        s.write(b"de")
        assert s.tell() == 5

    def test_partial_read_within_chunk(self) -> None:
        s = _QueueStream()
        s.write(b"abcdef")
        s.close_write()
        assert s.read(3) == b"abc"
        assert s.read(3) == b"def"
        assert s.read(1) == b""

    def test_backpressure_blocks_writer(self) -> None:
        s = _QueueStream(maxsize=1)
        written = []

        def writer() -> None:
            s.write(b"a" * 8)  # fills the queue
            s.write(b"b" * 8)  # blocks until reader consumes first chunk
            written.append("second_written")
            s.close_write()

        t = threading.Thread(target=writer, daemon=True)
        t.start()
        time.sleep(0.05)  # let writer fill queue and block
        s.read(8)  # consume first chunk — unblocks writer
        t.join(timeout=1.0)
        assert "second_written" in written

    def test_concurrent_write_read(self) -> None:
        s = _QueueStream()
        chunks = [b"x" * 1024 for _ in range(10)]
        result: list = []

        def writer() -> None:
            for c in chunks:
                s.write(c)
            s.close_write()

        threading.Thread(target=writer).start()
        while True:
            data = s.read(1024)
            if not data:
                break
            result.append(data)

        assert b"".join(result) == b"".join(chunks)
