import time
from collections import deque
from typing import Callable, Deque, Optional, Protocol, Tuple

from .progress import FileStatus


class ProgressReporter(Protocol):
    def on_total(self, total_files: int, total_bytes: int) -> None: ...

    def on_file_start(self, file_id: str, name: str, size: int, phase: str) -> None: ...

    def on_file_bytes(self, file_id: str, bytes_done: int) -> None: ...

    def on_file_phase(self, file_id: str, phase: str) -> None: ...

    def on_file_done(self, file_id: str, status: FileStatus) -> None: ...


class NullReporter:
    """No-op reporter; the engine's default so it stays headless and testable."""

    def on_total(self, total_files: int, total_bytes: int) -> None:
        pass

    def on_file_start(self, file_id: str, name: str, size: int, phase: str) -> None:
        pass

    def on_file_bytes(self, file_id: str, bytes_done: int) -> None:
        pass

    def on_file_phase(self, file_id: str, phase: str) -> None:
        pass

    def on_file_done(self, file_id: str, status: FileStatus) -> None:
        pass


class RollingRate:
    """Throughput/ETA from a rolling window of cumulative-byte samples.

    A rolling window (rather than a cumulative average) keeps the estimate from
    whipsawing when a large transfer starts or finishes.
    """

    def __init__(
        self,
        window_seconds: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._window = window_seconds
        self._clock = clock
        self._samples: Deque[Tuple[float, int]] = deque()

    def record(self, total_bytes: int) -> None:
        now = self._clock()
        self._samples.append((now, total_bytes))
        self._evict(now)

    def _evict(self, now: float) -> None:
        cutoff = now - self._window
        while len(self._samples) > 1 and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def bytes_per_second(self) -> float:
        if len(self._samples) < 2:
            return 0.0
        t0, b0 = self._samples[0]
        t1, b1 = self._samples[-1]
        dt = t1 - t0
        if dt <= 0:
            return 0.0
        rate = (b1 - b0) / dt
        return rate if rate > 0 else 0.0

    def mbps(self) -> float:
        return self.bytes_per_second() / (1024 * 1024)

    def eta_seconds(self, remaining_bytes: int) -> Optional[float]:
        if remaining_bytes <= 0:
            return 0.0
        rate = self.bytes_per_second()
        if rate <= 0:
            return None
        return remaining_bytes / rate
