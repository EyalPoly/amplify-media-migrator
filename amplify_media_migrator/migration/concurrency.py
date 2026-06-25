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
