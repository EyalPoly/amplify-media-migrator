import asyncio
from typing import Optional


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 10.0,
        burst_size: int = 10,
    ) -> None:
        self._requests_per_second = requests_per_second
        self._burst_size = burst_size
        self._tokens: float = burst_size
        self._last_update: Optional[float] = None
        self._lock: Optional[asyncio.Lock] = None

    async def acquire(self) -> None:
        raise NotImplementedError

    async def __aenter__(self) -> "RateLimiter":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        pass