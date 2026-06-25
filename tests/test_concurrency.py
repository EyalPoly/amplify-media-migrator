import asyncio
import threading

import pytest

from amplify_media_migrator.migration.concurrency import ThroughputMeter

pytestmark = pytest.mark.unit


class TestThroughputMeter:
    def test_starts_at_zero(self) -> None:
        assert ThroughputMeter().total() == 0

    def test_add_accumulates(self) -> None:
        m = ThroughputMeter()
        m.add(100)
        m.add(50)
        assert m.total() == 150

    def test_concurrent_adds_are_thread_safe(self) -> None:
        m = ThroughputMeter()

        def worker() -> None:
            for _ in range(1000):
                m.add(1)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert m.total() == 8000
