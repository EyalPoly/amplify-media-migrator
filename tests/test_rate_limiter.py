import asyncio
import time

import pytest

from amplify_media_migrator.utils.rate_limiter import RateLimiter


class TestRateLimiterInit:
    def test_default_values(self):
        limiter = RateLimiter()
        assert limiter._requests_per_second == 10.0
        assert limiter._burst_size == 10
        assert limiter._tokens == 10.0
        assert limiter._last_update is None
        assert limiter._lock is None

    def test_custom_values(self):
        limiter = RateLimiter(requests_per_second=5.0, burst_size=20)
        assert limiter._requests_per_second == 5.0
        assert limiter._burst_size == 20
        assert limiter._tokens == 20.0


class TestRateLimiterAcquire:
    @pytest.mark.asyncio
    async def test_first_acquire_immediate(self):
        limiter = RateLimiter(requests_per_second=10.0, burst_size=10)
        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.05
        assert limiter._tokens == 9.0

    @pytest.mark.asyncio
    async def test_burst_acquires_immediate(self):
        limiter = RateLimiter(requests_per_second=10.0, burst_size=5)
        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1
        assert limiter._tokens < 1.0

    @pytest.mark.asyncio
    async def test_waits_when_tokens_exhausted(self):
        limiter = RateLimiter(requests_per_second=10.0, burst_size=1)
        await limiter.acquire()

        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        assert elapsed >= 0.09

    @pytest.mark.asyncio
    async def test_tokens_refill_over_time(self):
        limiter = RateLimiter(requests_per_second=10.0, burst_size=2)
        await limiter.acquire()
        await limiter.acquire()
        assert limiter._tokens < 1.0

        await asyncio.sleep(0.15)

        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.05

    @pytest.mark.asyncio
    async def test_tokens_cap_at_burst_size(self):
        limiter = RateLimiter(requests_per_second=10.0, burst_size=3)
        await limiter.acquire()
        assert limiter._tokens < 3.0

        await asyncio.sleep(0.5)
        await limiter.acquire()

        assert limiter._tokens <= 3.0

    @pytest.mark.asyncio
    async def test_concurrent_acquires_respect_rate(self):
        limiter = RateLimiter(requests_per_second=10.0, burst_size=2)

        async def acquire_task():
            await limiter.acquire()

        start = time.monotonic()
        await asyncio.gather(*[acquire_task() for _ in range(4)])
        elapsed = time.monotonic() - start

        assert elapsed >= 0.18


class TestRateLimiterContextManager:
    @pytest.mark.asyncio
    async def test_context_manager_acquires(self):
        limiter = RateLimiter(requests_per_second=10.0, burst_size=10)
        async with limiter:
            pass
        assert limiter._tokens == 9.0

    @pytest.mark.asyncio
    async def test_context_manager_returns_limiter(self):
        limiter = RateLimiter()
        async with limiter as ctx:
            assert ctx is limiter


class TestRateLimiterEdgeCases:
    @pytest.mark.asyncio
    async def test_low_rate_high_burst(self):
        limiter = RateLimiter(requests_per_second=1.0, burst_size=5)
        start = time.monotonic()
        for _ in range(5):
            await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 0.9

    @pytest.mark.asyncio
    async def test_high_rate_low_burst(self):
        limiter = RateLimiter(requests_per_second=100.0, burst_size=1)
        await limiter.acquire()

        start = time.monotonic()
        await limiter.acquire()
        elapsed = time.monotonic() - start

        assert elapsed >= 0.009
        assert elapsed < 0.02

    @pytest.mark.asyncio
    async def test_fractional_tokens(self):
        limiter = RateLimiter(requests_per_second=10.0, burst_size=1)
        await limiter.acquire()

        await asyncio.sleep(0.05)

        assert limiter._last_update is not None
        await limiter.acquire()

    @pytest.mark.asyncio
    async def test_multiple_limiters_independent(self):
        limiter1 = RateLimiter(requests_per_second=10.0, burst_size=1)
        limiter2 = RateLimiter(requests_per_second=10.0, burst_size=5)

        await limiter1.acquire()
        await limiter2.acquire()

        assert limiter1._tokens < 1.0
        assert limiter2._tokens >= 4.0
