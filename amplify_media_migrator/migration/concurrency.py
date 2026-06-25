import asyncio
import threading
import time
from typing import Callable, Optional


class ThroughputMeter:
    """Thread-safe cumulative byte counter fed from worker threads."""

    def __init__(self) -> None:
        self._total = 0
        self._lock = threading.Lock()

    def add(self, delta: int) -> None:
        if delta <= 0:
            return
        with self._lock:
            self._total += delta

    def total(self) -> int:
        with self._lock:
            return self._total


class _BudgetReservation:
    def __init__(self, budget: "InflightBudget", amount: int) -> None:
        self._budget = budget
        self._amount = amount

    async def __aenter__(self) -> "None":
        await self._budget._acquire(self._amount)

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self._budget._release(self._amount)


class InflightBudget:
    """Async ceiling on concurrently-reserved bytes.

    A reservation larger than the whole budget is capped to the budget so a
    single oversized file can still pass instead of deadlocking.
    """

    def __init__(self, max_bytes: int) -> None:
        self._max = max_bytes
        self._used = 0
        self._cond: Optional[asyncio.Condition] = None

    def _ensure_cond(self) -> asyncio.Condition:
        # Bind the Condition to the running loop lazily (3.9-safe).
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    def available(self) -> int:
        return self._max - self._used

    def reserve(self, n: int) -> _BudgetReservation:
        return _BudgetReservation(self, min(n, self._max))

    async def _acquire(self, n: int) -> None:
        cond = self._ensure_cond()
        async with cond:
            while self._used + n > self._max:
                await cond.wait()
            self._used += n

    async def _release(self, n: int) -> None:
        cond = self._ensure_cond()
        async with cond:
            self._used -= n
            cond.notify_all()
