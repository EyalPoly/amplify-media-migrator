import threading
import time
from typing import Optional


class RateLimiter:
    def __init__(
        self,
        requests_per_second: float = 200.0,
        burst_size: int = 200,
    ) -> None:
        self._requests_per_second = requests_per_second
        self._burst_size = burst_size
        self._tokens: float = float(burst_size)
        self._last_update: Optional[float] = None
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
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

        time.sleep(wait_time)

    def __enter__(self) -> "RateLimiter":
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        pass
