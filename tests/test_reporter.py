import pytest

from amplify_media_migrator.migration.progress import FileStatus
from amplify_media_migrator.migration.reporter import NullReporter, RollingRate

pytestmark = pytest.mark.unit


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestRollingRate:
    def test_no_samples_is_zero(self) -> None:
        rate = RollingRate(clock=FakeClock())
        assert rate.bytes_per_second() == 0.0
        assert rate.mbps() == 0.0

    def test_single_sample_is_zero(self) -> None:
        clock = FakeClock()
        rate = RollingRate(clock=clock)
        rate.record(100)
        assert rate.bytes_per_second() == 0.0

    def test_steady_rate(self) -> None:
        clock = FakeClock()
        rate = RollingRate(window_seconds=30, clock=clock)
        rate.record(0)
        clock.advance(10)
        rate.record(10 * 1024 * 1024)
        assert rate.mbps() == pytest.approx(1.0, rel=1e-3)

    def test_eta(self) -> None:
        clock = FakeClock()
        rate = RollingRate(window_seconds=30, clock=clock)
        rate.record(0)
        clock.advance(1)
        rate.record(1000)
        # 1000 B/s, 5000 remaining -> 5 s
        assert rate.eta_seconds(5000) == pytest.approx(5.0)

    def test_eta_zero_when_nothing_remaining(self) -> None:
        rate = RollingRate(clock=FakeClock())
        assert rate.eta_seconds(0) == 0.0

    def test_eta_none_when_no_throughput(self) -> None:
        clock = FakeClock()
        rate = RollingRate(clock=clock)
        rate.record(100)
        clock.advance(5)
        rate.record(100)  # no progress
        assert rate.eta_seconds(1000) is None

    def test_window_evicts_old_samples_no_whipsaw(self) -> None:
        clock = FakeClock()
        rate = RollingRate(window_seconds=30, clock=clock)
        # A burst long ago should not inflate the current rate.
        rate.record(0)
        clock.advance(1)
        rate.record(100 * 1024 * 1024)  # huge burst
        clock.advance(40)  # window passes
        rate.record(100 * 1024 * 1024)  # idle since
        clock.advance(10)
        rate.record(100 * 1024 * 1024)
        # Only the recent (flat) samples remain -> ~0 MB/s
        assert rate.mbps() == pytest.approx(0.0, abs=1e-6)

    def test_negative_rate_guarded(self) -> None:
        clock = FakeClock()
        rate = RollingRate(window_seconds=30, clock=clock)
        rate.record(1000)
        clock.advance(1)
        rate.record(500)  # counter went down
        assert rate.bytes_per_second() == 0.0


class TestNullReporter:
    def test_all_methods_are_noops(self) -> None:
        reporter = NullReporter()
        reporter.on_total(1, 2)
        reporter.on_file_start("id", "name", 10, "querying")
        reporter.on_file_bytes("id", 5)
        reporter.on_file_phase("id", "uploading")
        reporter.on_file_done("id", FileStatus.COMPLETED)

    def test_null_reporter_accepts_concurrency(self) -> None:
        NullReporter().on_concurrency(7)
