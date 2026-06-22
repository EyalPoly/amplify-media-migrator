import pytest
from rich.console import Console

from amplify_media_migrator.cli_progress import (
    LiveReporter,
    format_bytes,
    format_duration,
    format_eta,
)
from amplify_media_migrator.migration.progress import FileStatus

pytestmark = pytest.mark.unit


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class TestFormatters:
    def test_format_bytes(self) -> None:
        assert format_bytes(512) == "512 B"
        assert format_bytes(2048) == "2 KB"
        assert format_bytes(5 * 1024 * 1024) == "5 MB"
        assert format_bytes(3 * 1024**3) == "3.0 GB"

    def test_format_duration(self) -> None:
        assert format_duration(0) == "0:00:00"
        assert format_duration(65) == "0:01:05"
        assert format_duration(3661) == "1:01:01"

    def test_format_eta_none(self) -> None:
        assert format_eta(None) == "--:--:--"
        assert format_eta(90) == "0:01:30"


class TestLiveReporterAccounting:
    def test_only_uploading_bytes_count_toward_global(self) -> None:
        r = LiveReporter(clock=FakeClock())
        r.on_total(1, 1000)
        r.on_file_start("a", "a.jpg", 100, "querying")
        r.on_file_phase("a", "downloading")
        r.on_file_bytes("a", 50)
        assert r._global_done_locked() == 0

        r.on_file_phase("a", "uploading")
        r.on_file_bytes("a", 70)
        assert r._global_done_locked() == 70

    def test_size_credited_once_on_completion(self) -> None:
        r = LiveReporter(clock=FakeClock())
        r.on_total(1, 1000)
        r.on_file_start("a", "a.jpg", 600, "uploading")
        r.on_file_bytes("a", 300)
        assert r._global_done_locked() == 300

        r.on_file_done("a", FileStatus.COMPLETED)
        # in-flight bytes removed, full size credited exactly once
        assert r._global_done_locked() == 600

    def test_orphan_does_not_credit_bytes(self) -> None:
        r = LiveReporter(clock=FakeClock())
        r.on_total(1, 1000)
        r.on_file_start("a", "a.jpg", 600, "querying")
        r.on_file_done("a", FileStatus.ORPHAN)
        assert r._global_done_locked() == 0

    def test_partial_credits_size(self) -> None:
        r = LiveReporter(clock=FakeClock())
        r.on_total(1, 1000)
        r.on_file_start("a", "a.jpg", 600, "uploading")
        r.on_file_done("a", FileStatus.PARTIAL)
        assert r._global_done_locked() == 600

    def test_phase_change_resets_per_file_bytes(self) -> None:
        r = LiveReporter(clock=FakeClock())
        r.on_total(1, 1000)
        r.on_file_start("a", "a.jpg", 600, "downloading")
        r.on_file_bytes("a", 400)
        r.on_file_phase("a", "uploading")
        assert r._global_done_locked() == 0


class TestLiveReporterRender:
    def _populate(self, n: int) -> LiveReporter:
        r = LiveReporter(clock=FakeClock())
        r.on_total(n, n * 1000)
        for i in range(n):
            fid = f"f{i}"
            r.on_file_start(fid, f"{1000 + i}.mp4", (i + 1) * 1000, "querying")
            r.on_file_phase(fid, "uploading")
            r.on_file_bytes(fid, (i + 1) * 100)
        return r

    def test_render_caps_active_rows_and_shows_overflow(self) -> None:
        r = self._populate(12)
        console = Console(record=True, width=120)
        console.print(r.render())
        out = console.export_text()
        assert "+4 more" in out  # 12 - 8 cap
        assert "MB/s" in out
        assert "ETA" in out
        # Largest file (sorted size desc) is shown
        assert "1011.mp4" in out

    def test_render_orders_by_size_desc(self) -> None:
        r = self._populate(12)
        console = Console(record=True, width=120)
        console.print(r.render())
        out = console.export_text()
        # The 4 smallest are dropped beyond the cap of 8.
        assert "1000.mp4" not in out
        assert "1003.mp4" not in out

    def test_render_no_active(self) -> None:
        r = LiveReporter(clock=FakeClock())
        r.on_total(0, 0)
        console = Console(record=True, width=120)
        console.print(r.render())
        out = console.export_text()
        assert "Migrating" in out

    def test_plain_line(self) -> None:
        clock = FakeClock()
        r = LiveReporter(clock=clock)
        r.on_total(2, 1000)
        r.on_file_start("a", "a.jpg", 500, "uploading")
        r.on_file_bytes("a", 0)
        r.sample()
        clock.advance(10)
        r.on_file_bytes("a", 500)
        r.sample()
        line = r.plain_line()
        assert "Migrating" in line
        assert "MB/s" in line
        assert "total 2" in line
        assert "1 active" in line
