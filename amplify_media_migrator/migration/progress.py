from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, List


class FileStatus(Enum):
    PENDING = "pending"
    DOWNLOADED = "downloaded"
    UPLOADED = "uploaded"
    COMPLETED = "completed"
    FAILED = "failed"
    ORPHAN = "orphan"
    NEEDS_REVIEW = "needs_review"
    PARTIAL = "partial"


@dataclass
class FileProgress:
    filename: str
    status: FileStatus
    sequential_ids: List[int] = field(default_factory=list)
    observation_ids: List[str] = field(default_factory=list)
    s3_url: Optional[str] = None
    media_ids: List[str] = field(default_factory=list)
    error: Optional[str] = None
    updated_at: Optional[datetime] = None


@dataclass
class ProgressSummary:
    pending: int = 0
    downloaded: int = 0
    uploaded: int = 0
    completed: int = 0
    failed: int = 0
    orphan: int = 0
    needs_review: int = 0
    partial: int = 0


class ProgressTracker:
    def __init__(self, progress_path: Optional[Path] = None) -> None:
        self._progress_path = progress_path
        self._folder_id: Optional[str] = None
        self._started_at: Optional[datetime] = None
        self._updated_at: Optional[datetime] = None
        self._total_files: int = 0
        self._files: Dict[str, FileProgress] = {}

    def load(self, folder_id: str) -> bool:
        raise NotImplementedError

    def save(self) -> None:
        raise NotImplementedError

    def set_total_files(self, total: int) -> None:
        self._total_files = total

    def update_file(
        self,
        file_id: str,
        filename: str,
        status: FileStatus,
        sequential_ids: Optional[List[int]] = None,
        observation_ids: Optional[List[str]] = None,
        s3_url: Optional[str] = None,
        media_ids: Optional[List[str]] = None,
        error: Optional[str] = None,
    ) -> None:
        raise NotImplementedError

    def get_file(self, file_id: str) -> Optional[FileProgress]:
        return self._files.get(file_id)

    def get_files_by_status(self, status: FileStatus) -> List[FileProgress]:
        raise NotImplementedError

    def get_summary(self) -> ProgressSummary:
        raise NotImplementedError

    def get_pending_file_ids(self) -> List[str]:
        raise NotImplementedError

    def get_failed_file_ids(self) -> List[str]:
        raise NotImplementedError

    def export_to_json(self, status: FileStatus, output_path: Path) -> None:
        raise NotImplementedError