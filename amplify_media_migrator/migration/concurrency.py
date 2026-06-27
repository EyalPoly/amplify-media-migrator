import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class AdaptiveSettings:
    """Runtime tunables for adaptive concurrency and the in-flight memory cap."""

    enabled: bool = True
    min_workers: int = 4
    initial_workers: Optional[int] = None
    max_inflight_buffer_mb: int = 512
    window_seconds: float = 10.0


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


class ConcurrencyController:
    """Tunes an active-worker limit from error and throughput signals.

    Shrinks hard on retryable errors (multiplicative decrease + cooldown) and
    gently when extra workers stop improving throughput (the bandwidth-bound
    case, which produces no errors). Grows while throughput keeps climbing.
    A throughput drop only grows back when the previous step was a shrink (the
    shrink is the likely cause); a drop after a grow backs off instead, so noisy
    bandwidth-bound throughput can't ratchet the limit up to the ceiling.
    """

    def __init__(
        self,
        min_workers: int,
        max_workers: int,
        initial: int,
        *,
        step: int = 2,
        decrease_factor: float = 0.5,
        hysteresis: float = 0.15,
        cooldown_windows: int = 3,
    ) -> None:
        self._min = min_workers
        self._max = max_workers
        self._step = step
        self._decrease_factor = decrease_factor
        self._hysteresis = hysteresis
        self._cooldown_windows = cooldown_windows

        self._limit: float = float(max(min_workers, min(max_workers, initial)))
        self._prev_throughput: Optional[float] = None
        self._last_step_was_shrink = False
        self._cooldown = 0

        self._active = 0
        self._errors_since_window = 0
        self._error_lock = threading.Lock()
        self._cond: Optional[asyncio.Condition] = None

    def current_limit(self) -> int:
        return int(round(self._limit))

    def record_retryable_error(self) -> None:
        with self._error_lock:
            self._errors_since_window += 1

    def _take_errors(self) -> int:
        with self._error_lock:
            n = self._errors_since_window
            self._errors_since_window = 0
            return n

    def step(self, errors: int, throughput: float) -> None:
        if self._cooldown > 0:
            self._cooldown -= 1

        if errors > 0:
            self._limit = max(self._min, self._limit * self._decrease_factor)
            self._cooldown = self._cooldown_windows
            self._last_step_was_shrink = False
        elif self._cooldown == 0:
            prev = self._prev_throughput
            if prev is not None and prev > 0:
                change = (throughput - prev) / prev
                if change > self._hysteresis:
                    self._grow()
                elif change < -self._hysteresis and self._last_step_was_shrink:
                    self._grow()
                else:
                    self._shrink()
            else:
                self._grow()

        self._limit = max(self._min, min(self._max, self._limit))
        self._prev_throughput = throughput

    def _grow(self) -> None:
        self._limit = min(self._max, self._limit + self._step)
        self._last_step_was_shrink = False

    def _shrink(self) -> None:
        self._limit = max(self._min, self._limit - self._step)
        self._last_step_was_shrink = True

    def _ensure_cond(self) -> asyncio.Condition:
        if self._cond is None:
            self._cond = asyncio.Condition()
        return self._cond

    async def acquire(self) -> None:
        cond = self._ensure_cond()
        async with cond:
            while self._active >= self.current_limit():
                await cond.wait()
            self._active += 1

    async def release(self) -> None:
        cond = self._ensure_cond()
        async with cond:
            self._active -= 1
            cond.notify_all()

    async def notify_waiters(self) -> None:
        cond = self._ensure_cond()
        async with cond:
            cond.notify_all()

    async def run(
        self,
        meter: ThroughputMeter,
        stop: asyncio.Event,
        window_seconds: float,
        clock: Callable[[], float] = time.monotonic,
        on_limit: Optional[Callable[[int], None]] = None,
    ) -> None:
        last_total = meter.total()
        last_time = clock()
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=window_seconds)
            except asyncio.TimeoutError:
                pass
            if stop.is_set():
                return
            now = clock()
            total = meter.total()
            dt = now - last_time
            rate = (total - last_total) / dt if dt > 0 else 0.0
            self.step(self._take_errors(), rate)
            if on_limit is not None:
                on_limit(self.current_limit())
            await self.notify_waiters()
            last_total, last_time = total, now
