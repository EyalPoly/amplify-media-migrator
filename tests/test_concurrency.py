import asyncio
import threading

import pytest

from amplify_media_migrator.migration.concurrency import (
    ConcurrencyController,
    InflightBudget,
    ThroughputMeter,
)

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


def _controller(initial: int = 10, lo: int = 4, hi: int = 50) -> ConcurrencyController:
    return ConcurrencyController(min_workers=lo, max_workers=hi, initial=initial)


class TestControllerStep:
    def test_error_halves_limit(self) -> None:
        c = _controller(initial=20)
        c.step(errors=1, throughput=1000.0)
        assert c.current_limit() == 10

    def test_error_never_below_min(self) -> None:
        c = _controller(initial=6, lo=4)
        c.step(errors=3, throughput=0.0)
        assert c.current_limit() == 4

    def test_increase_when_throughput_climbs(self) -> None:
        c = _controller(initial=10)
        c.step(errors=0, throughput=1000.0)  # establishes baseline, +2
        c.step(errors=0, throughput=2000.0)  # climbing, +2
        assert c.current_limit() == 14

    def test_plateau_steps_down(self) -> None:
        c = _controller(initial=20)
        c.step(errors=0, throughput=1000.0)  # baseline, +2 -> 22
        c.step(errors=0, throughput=1000.0)  # flat -> -2 -> 20
        assert c.current_limit() == 20

    def test_drop_after_growth_steps_down(self) -> None:
        c = _controller(initial=20)
        c.step(errors=0, throughput=2000.0)  # baseline grow -> 22
        c.step(errors=0, throughput=1000.0)  # drop after a grow -> back off -> 20
        assert c.current_limit() == 20

    def test_drop_after_shrink_recovers(self) -> None:
        c = _controller(initial=20)
        c.step(errors=0, throughput=2000.0)  # baseline grow -> 22
        c.step(errors=0, throughput=2000.0)  # flat -> shrink -> 20
        c.step(errors=0, throughput=1000.0)  # drop after a shrink -> recover -> 22
        assert c.current_limit() == 22

    def test_noisy_throughput_does_not_ratchet_up(self) -> None:
        c = _controller(initial=10)
        for tput in [1000.0, 3000.0] * 4:
            c.step(errors=0, throughput=tput)
        assert c.current_limit() == 14

    def test_error_triggers_cooldown_blocking_increase(self) -> None:
        c = _controller(initial=20)
        c.step(errors=1, throughput=2000.0)  # -> 10, cooldown=3
        c.step(errors=0, throughput=5000.0)  # cooldown, no increase
        assert c.current_limit() == 10

    def test_increase_capped_at_max(self) -> None:
        c = _controller(initial=49, hi=50)
        c.step(errors=0, throughput=1000.0)
        c.step(errors=0, throughput=9000.0)
        assert c.current_limit() == 50


class TestControllerGate:
    async def test_limit_blocks_excess_acquires(self) -> None:
        c = _controller(initial=2, lo=1, hi=10)
        await c.acquire()
        await c.acquire()
        third = asyncio.ensure_future(c.acquire())
        await asyncio.sleep(0.02)
        assert not third.done()
        await c.release()
        await asyncio.sleep(0.02)
        assert third.done()
        await third

    async def test_raising_limit_wakes_parked_acquirer(self) -> None:
        c = _controller(initial=1, lo=1, hi=10)
        await c.acquire()
        waiter = asyncio.ensure_future(c.acquire())
        await asyncio.sleep(0.02)
        assert not waiter.done()
        c.step(errors=0, throughput=1000.0)  # baseline -> limit 3
        await c.notify_waiters()
        await asyncio.sleep(0.02)
        assert waiter.done()
        await waiter
