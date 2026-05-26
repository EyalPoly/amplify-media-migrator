import threading
import time

import pytest

from amplify_media_migrator.utils.rate_limiter import RateLimiter


class TestRateLimiterInit:
    def test_default_values(self) -> None:
        limiter = RateLimiter()
        assert limiter._requests_per_second == 200.0
        assert limiter._burst_size == 200
        assert limiter._tokens == 200.0
        assert limiter._last_update is None

    def test_custom_values(self) -> None:
        limiter = RateLimiter(requests_per_second=5.0, burst_size=20)
        assert limiter._requests_per_second == 5.0
        assert limiter._burst_size == 20
        assert limiter._tokens == 20.0


class TestRateLimiterAcquire:
    def test_acquire_is_sync(self) -> None:
        import inspect

        limiter = RateLimiter(requests_per_second=10.0, burst_size=10)
        result = limiter.acquire()
        assert result is None
        assert not inspect.isawaitable(result)

    def test_first_acquire_immediate(self) -> None:
        limiter = RateLimiter(requests_per_second=10.0, burst_size=10)
        start = time.monotonic()
        limiter.acquire()
        assert time.monotonic() - start < 0.05
        assert limiter._tokens == 9.0

    def test_burst_acquires_immediate(self) -> None:
        limiter = RateLimiter(requests_per_second=10.0, burst_size=5)
        start = time.monotonic()
        for _ in range(5):
            limiter.acquire()
        assert time.monotonic() - start < 0.1
        assert limiter._tokens < 1.0

    def test_waits_when_tokens_exhausted(self) -> None:
        limiter = RateLimiter(requests_per_second=10.0, burst_size=1)
        limiter.acquire()
        start = time.monotonic()
        limiter.acquire()
        assert time.monotonic() - start >= 0.09

    def test_tokens_refill_over_time(self) -> None:
        limiter = RateLimiter(requests_per_second=10.0, burst_size=2)
        limiter.acquire()
        limiter.acquire()
        time.sleep(0.15)
        start = time.monotonic()
        limiter.acquire()
        assert time.monotonic() - start < 0.05

    def test_tokens_cap_at_burst_size(self) -> None:
        limiter = RateLimiter(requests_per_second=10.0, burst_size=3)
        limiter.acquire()
        time.sleep(0.5)
        limiter.acquire()
        assert limiter._tokens <= 3.0

    def test_concurrent_acquires_respect_rate(self) -> None:
        limiter = RateLimiter(requests_per_second=10.0, burst_size=2)
        results: list = []

        def do_acquire() -> None:
            limiter.acquire()
            results.append(time.monotonic())

        start = time.monotonic()
        threads = [threading.Thread(target=do_acquire) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert max(results) - start >= 0.18


class TestRateLimiterContextManager:
    def test_context_manager_acquires(self) -> None:
        limiter = RateLimiter(requests_per_second=10.0, burst_size=10)
        with limiter:
            pass
        assert limiter._tokens == 9.0

    def test_context_manager_returns_limiter(self) -> None:
        limiter = RateLimiter()
        with limiter as ctx:
            assert ctx is limiter


class TestRateLimiterEdgeCases:
    def test_low_rate_high_burst(self) -> None:
        limiter = RateLimiter(requests_per_second=1.0, burst_size=5)
        start = time.monotonic()
        for _ in range(5):
            limiter.acquire()
        assert time.monotonic() - start < 0.1
        start = time.monotonic()
        limiter.acquire()
        assert time.monotonic() - start >= 0.9

    def test_multiple_limiters_independent(self) -> None:
        limiter1 = RateLimiter(requests_per_second=10.0, burst_size=1)
        limiter2 = RateLimiter(requests_per_second=10.0, burst_size=5)
        limiter1.acquire()
        limiter2.acquire()
        assert limiter1._tokens < 1.0
        assert limiter2._tokens >= 4.0
