import asyncio
import time
from typing import Optional


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 10.0,
        burst_size: int = 10,
    ) -> None:
        self._requests_per_second = requests_per_second
        self._burst_size = burst_size
        self._tokens: float = float(burst_size)
        self._last_update: Optional[float] = None
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def acquire(self) -> None:
        async with self._get_lock():
            now = time.monotonic()

            if self._last_update is None:
                self._last_update = now

            elapsed = now - self._last_update
            self._tokens = min(
                self._burst_size,
                self._tokens + elapsed * self._requests_per_second,
            )
            self._last_update = now

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return

            wait_time = (1.0 - self._tokens) / self._requests_per_second
            self._tokens = 0.0
            self._last_update = now + wait_time

        await asyncio.sleep(wait_time)

    async def __aenter__(self) -> "RateLimiter":
        await self.acquire()
        return self

    async def __aexit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> None:
        pass
