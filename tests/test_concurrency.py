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


from amplify_media_migrator.migration.concurrency import InflightBudget


class TestInflightBudget:
    async def test_reserve_reduces_available(self) -> None:
        budget = InflightBudget(1000)
        async with budget.reserve(400):
            assert budget.available() == 600
        assert budget.available() == 1000

    async def test_over_budget_reservation_parks_until_release(self) -> None:
        budget = InflightBudget(1000)
        order: list[str] = []

        async def big() -> None:
            async with budget.reserve(800):
                order.append("big-acquired")
                await asyncio.sleep(0.05)
            order.append("big-released")

        async def second() -> None:
            await asyncio.sleep(0.01)
            async with budget.reserve(800):
                order.append("second-acquired")

        await asyncio.gather(big(), second())
        assert order == ["big-acquired", "big-released", "second-acquired"]

    async def test_reservation_larger_than_budget_is_capped(self) -> None:
        budget = InflightBudget(500)
        async with budget.reserve(900):
            assert budget.available() == 0
        assert budget.available() == 500
