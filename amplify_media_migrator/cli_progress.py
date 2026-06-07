import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from rich.console import Group, RenderableType
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

from .migration.progress import FileStatus
from .migration.reporter import RollingRate

ACTIVE_ROW_CAP = 8
_GLOBAL_PHASE = "uploading"


def format_bytes(num: float) -> str:
    if num >= 1024**3:
        return f"{num / 1024 ** 3:.1f} GB"
    if num >= 1024**2:
        return f"{num / 1024 ** 2:.0f} MB"
    if num >= 1024:
        return f"{num / 1024:.0f} KB"
    return f"{int(num)} B"


def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    return f"{hours}:{minutes:02d}:{secs:02d}"


def format_eta(eta: Optional[float]) -> str:
    if eta is None:
        return "--:--:--"
    return format_duration(eta)


@dataclass
class _FileState:
    name: str
    size: int
    phase: str
    bytes_done: int = 0
    rate: RollingRate = field(default_factory=RollingRate)


class LiveReporter:
    """Thread-safe aggregator implementing ProgressReporter.

    Byte callbacks fire from many worker threads; this only mutates an in-memory
    state map under a lock. A ticker thread renders from a snapshot of that state.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._total_files = 0
        self._total_bytes = 0
        self._completed_bytes = 0
        self._counts: Dict[str, int] = {}
        self._active: Dict[str, _FileState] = {}
        self._global_rate = RollingRate(clock=clock)
        self._start = clock()

    def on_total(self, total_files: int, total_bytes: int) -> None:
        with self._lock:
            self._total_files = total_files
            self._total_bytes = total_bytes

    def on_file_start(self, file_id: str, name: str, size: int, phase: str) -> None:
        with self._lock:
            self._active[file_id] = _FileState(
                name=name,
                size=size,
                phase=phase,
                rate=RollingRate(clock=self._clock),
            )

    def on_file_bytes(self, file_id: str, bytes_done: int) -> None:
        with self._lock:
            state = self._active.get(file_id)
            if state is not None:
                state.bytes_done = bytes_done

    def on_file_phase(self, file_id: str, phase: str) -> None:
        with self._lock:
            state = self._active.get(file_id)
            if state is not None:
                state.phase = phase
                state.bytes_done = 0
                state.rate = RollingRate(clock=self._clock)

    def on_file_done(self, file_id: str, status: FileStatus) -> None:
        with self._lock:
            state = self._active.pop(file_id, None)
            self._counts[status.value] = self._counts.get(status.value, 0) + 1
            if state is not None and status in (
                FileStatus.COMPLETED,
                FileStatus.PARTIAL,
            ):
                self._completed_bytes += state.size

    def _global_done_locked(self) -> int:
        live = sum(
            s.bytes_done for s in self._active.values() if s.phase == _GLOBAL_PHASE
        )
        return self._completed_bytes + live

    def sample(self) -> None:
        """Feed current byte counts into the rolling-rate windows (ticker thread)."""
        with self._lock:
            self._global_rate.record(self._global_done_locked())
            for state in self._active.values():
                state.rate.record(state.bytes_done)

    def _snapshot(
        self,
    ) -> Tuple[int, int, int, Dict[str, int], List[_FileState], float]:
        done = self._global_done_locked()
        counts = dict(self._counts)
        active = sorted(self._active.values(), key=lambda s: s.size, reverse=True)
        mbps = self._global_rate.mbps()
        return done, self._total_bytes, self._total_files, counts, active, mbps

    def _eta(self, done: int, total: int) -> str:
        eta = self._global_rate.eta_seconds(max(total - done, 0))
        return format_eta(eta)

    def render(self) -> RenderableType:
        with self._lock:
            done, total, _total_files, counts, active, mbps = self._snapshot()
            eta = self._eta(done, total)
            elapsed = self._clock() - self._start
            n_active = len(self._active)
        pct = (done / total * 100) if total else 0.0

        header = Table.grid(padding=(0, 1))
        header.add_row(
            Text(f"Migrating {pct:.0f}%", style="bold"),
            ProgressBar(
                total=max(total, 1), completed=min(done, total or done), width=30
            ),
            Text(
                f"{format_bytes(done)}/{format_bytes(total)} · "
                f"{mbps:.1f} MB/s · ETA {eta}"
            ),
        )

        counts_line = Text(
            f"ok {counts.get('completed', 0)}  "
            f"fail {counts.get('failed', 0)}  "
            f"orphan {counts.get('orphan', 0)}  "
            f"review {counts.get('needs_review', 0)}       "
            f"elapsed {format_duration(elapsed)} · {n_active} active"
        )

        renderables: List[RenderableType] = [header, counts_line]

        if active:
            shown = active[:ACTIVE_ROW_CAP]
            heading = "Active"
            if len(active) > ACTIVE_ROW_CAP:
                heading += f"   (showing {len(shown)}/{len(active)})"
            renderables.append(Text(""))
            renderables.append(Text(heading, style="bold"))

            table = Table.grid(padding=(0, 2))
            for state in shown:
                fpct = (state.bytes_done / state.size * 100) if state.size else 0.0
                table.add_row(
                    Text(state.name),
                    ProgressBar(
                        total=max(state.size, 1),
                        completed=min(state.bytes_done, state.size or state.bytes_done),
                        width=20,
                    ),
                    Text(f"{fpct:.0f}%" if state.size else state.phase),
                    Text(format_bytes(state.bytes_done)),
                    Text(f"{state.rate.mbps():.1f} MB/s"),
                )
            renderables.append(table)
            if len(active) > ACTIVE_ROW_CAP:
                renderables.append(Text(f"  … +{len(active) - ACTIVE_ROW_CAP} more"))

        return Group(*renderables)

    def plain_line(self) -> str:
        with self._lock:
            done, total, _total_files, counts, _active, mbps = self._snapshot()
            eta = self._eta(done, total)
            n_active = len(self._active)
        pct = (done / total * 100) if total else 0.0
        return (
            f"Migrating {pct:.0f}% · {format_bytes(done)}/{format_bytes(total)} · "
            f"{mbps:.1f} MB/s · ETA {eta} · "
            f"ok {counts.get('completed', 0)} fail {counts.get('failed', 0)} "
            f"orphan {counts.get('orphan', 0)} · {n_active} active"
        )
