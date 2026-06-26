import ctypes
import logging
import os
import platform
import subprocess
from typing import Literal, Optional

logger = logging.getLogger(__name__)

_ES_CONTINUOUS = 0x80000000
_ES_SYSTEM_REQUIRED = 0x00000001
_ES_DISPLAY_REQUIRED = 0x00000002

_WINDOWS_HANDLE = "windows"


class KeepAwake:
    """Prevent the host machine from sleeping while the context is active.

    Failure to acquire an assertion never raises: the migration is already
    fully resumable, so a missing power assertion must not block a run.
    """

    def __init__(
        self, reason: str = "amplify-media-migrator migration in progress"
    ) -> None:
        self._reason = reason
        self._handle: Optional[object] = None

    def __enter__(self) -> "KeepAwake":
        system = platform.system()
        try:
            if system == "Darwin":
                self._handle = _start_macos()
                logger.info("Keeping system awake (caffeinate).")
            elif system == "Windows":
                self._handle = _start_windows()
                logger.info("Keeping system awake (SetThreadExecutionState).")
            elif system == "Linux":
                self._handle = _start_linux(self._reason)
                logger.info("Keeping system awake (systemd-inhibit).")
            else:
                logger.warning(
                    "Sleep prevention unavailable on platform %s - continuing.",
                    system,
                )
        except (FileNotFoundError, OSError, AttributeError) as e:
            self._handle = None
            logger.warning("Sleep prevention unavailable: %s - continuing.", e)
        return self

    def __exit__(self, *exc: object) -> Literal[False]:
        try:
            _stop(self._handle)
        except Exception as e:  # cleanup must never mask the wrapped exception
            logger.warning("Failed to release sleep prevention: %s", e)
        finally:
            self._handle = None
        return False


def _start_macos() -> subprocess.Popen:
    return subprocess.Popen(
        ["caffeinate", "-dimsu", "-w", str(os.getpid())],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _start_linux(reason: str) -> subprocess.Popen:
    return subprocess.Popen(
        [
            "systemd-inhibit",
            "--what=idle:sleep",
            "--who=amplify-media-migrator",
            f"--why={reason}",
            "--mode=block",
            "sleep",
            "infinity",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _start_windows() -> str:
    # SetThreadExecutionState is per-thread: the flags must be set and cleared on
    # the same thread. migrate enters and exits this context on the main thread.
    ctypes.windll.kernel32.SetThreadExecutionState(  # type: ignore[attr-defined]
        _ES_CONTINUOUS | _ES_SYSTEM_REQUIRED | _ES_DISPLAY_REQUIRED
    )
    return _WINDOWS_HANDLE


def _stop(handle: Optional[object]) -> None:
    if handle is None:
        return
    if handle == _WINDOWS_HANDLE:
        ctypes.windll.kernel32.SetThreadExecutionState(_ES_CONTINUOUS)  # type: ignore[attr-defined]
        return
    handle.terminate()  # type: ignore[attr-defined]
    try:
        handle.wait(timeout=5)  # type: ignore[attr-defined]  # reap the child so it never zombies
    except subprocess.TimeoutExpired:
        handle.kill()  # type: ignore[attr-defined]
