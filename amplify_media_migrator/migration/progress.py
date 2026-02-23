import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


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


DEFAULT_PROGRESS_DIR = Path.home() / ".amplify-media-migrator"


def _file_progress_to_dict(fp: FileProgress) -> Dict[str, Any]:
    return {
        "filename": fp.filename,
        "status": fp.status.value,
        "sequential_ids": fp.sequential_ids,
        "observation_ids": fp.observation_ids,
        "s3_url": fp.s3_url,
        "media_ids": fp.media_ids,
        "error": fp.error,
        "updated_at": fp.updated_at.isoformat() if fp.updated_at else None,
    }


def _file_progress_from_dict(data: Dict[str, Any]) -> FileProgress:
    updated_at = None
    if data.get("updated_at"):
        updated_at = datetime.fromisoformat(data["updated_at"])
    return FileProgress(
        filename=data["filename"],
        status=FileStatus(data["status"]),
        sequential_ids=data.get("sequential_ids", []),
        observation_ids=data.get("observation_ids", []),
        s3_url=data.get("s3_url"),
        media_ids=data.get("media_ids", []),
        error=data.get("error"),
        updated_at=updated_at,
    )


class ProgressTracker:
    def __init__(self, progress_dir: Optional[Path] = None) -> None:
        self._progress_dir = progress_dir or DEFAULT_PROGRESS_DIR
        self._folder_id: Optional[str] = None
        self._started_at: Optional[datetime] = None
        self._updated_at: Optional[datetime] = None
        self._total_files: int = 0
        self._files: Dict[str, FileProgress] = {}

    @property
    def progress_path(self) -> Optional[Path]:
        if self._folder_id is None:
            return None
        return self._progress_dir / f"progress_{self._folder_id}.json"

    @property
    def folder_id(self) -> Optional[str]:
        return self._folder_id

    @property
    def total_files(self) -> int:
        return self._total_files

    @property
    def files(self) -> Dict[str, FileProgress]:
        return self._files

    def load(self, folder_id: str) -> bool:
        self._folder_id = folder_id
        path = self.progress_path
        assert path is not None

        if not path.exists():
            self._started_at = datetime.now(timezone.utc)
            self._updated_at = self._started_at
            self._total_files = 0
            self._files = {}
            logger.info("No existing progress file for folder %s", folder_id)
            return False

        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("Failed to load progress file: %s", e)
            self._started_at = datetime.now(timezone.utc)
            self._updated_at = self._started_at
            self._total_files = 0
            self._files = {}
            return False

        self._started_at = datetime.fromisoformat(data["started_at"])
        self._updated_at = datetime.fromisoformat(data["updated_at"])
        self._total_files = data.get("total_files", 0)
        self._files = {
            file_id: _file_progress_from_dict(file_data)
            for file_id, file_data in data.get("files", {}).items()
        }
        logger.info(
            "Loaded progress for folder %s: %d files tracked",
            folder_id,
            len(self._files),
        )
        return True

    def save(self) -> None:
        path = self.progress_path
        if path is None:
            raise RuntimeError("Cannot save: no folder_id set. Call load() first.")

        self._updated_at = datetime.now(timezone.utc)
        path.parent.mkdir(parents=True, exist_ok=True)

        data = {
            "folder_id": self._folder_id,
            "started_at": self._started_at.isoformat() if self._started_at else None,
            "updated_at": self._updated_at.isoformat(),
            "total_files": self._total_files,
            "files": {
                file_id: _file_progress_to_dict(fp)
                for file_id, fp in self._files.items()
            },
            "summary": self._build_summary_dict(),
        }

        path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        logger.info("Progress saved to %s", path)

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
        existing = self._files.get(file_id)
        if existing:
            existing.status = status
            if sequential_ids is not None:
                existing.sequential_ids = sequential_ids
            if observation_ids is not None:
                existing.observation_ids = observation_ids
            if s3_url is not None:
                existing.s3_url = s3_url
            if media_ids is not None:
                existing.media_ids = media_ids
            existing.error = error
            existing.updated_at = datetime.now(timezone.utc)
        else:
            self._files[file_id] = FileProgress(
                filename=filename,
                status=status,
                sequential_ids=sequential_ids or [],
                observation_ids=observation_ids or [],
                s3_url=s3_url,
                media_ids=media_ids or [],
                error=error,
                updated_at=datetime.now(timezone.utc),
            )

    def get_file(self, file_id: str) -> Optional[FileProgress]:
        return self._files.get(file_id)

    def get_files_by_status(self, status: FileStatus) -> List[FileProgress]:
        return [fp for fp in self._files.values() if fp.status == status]

    def get_summary(self) -> ProgressSummary:
        summary = ProgressSummary()
        for fp in self._files.values():
            attr = fp.status.value
            setattr(summary, attr, getattr(summary, attr) + 1)
        return summary

    def get_pending_file_ids(self) -> List[str]:
        return [
            fid for fid, fp in self._files.items() if fp.status == FileStatus.PENDING
        ]

    def get_failed_file_ids(self) -> List[str]:
        return [
            fid for fid, fp in self._files.items() if fp.status == FileStatus.FAILED
        ]

    def get_partial_file_ids(self) -> List[str]:
        return [
            fid for fid, fp in self._files.items() if fp.status == FileStatus.PARTIAL
        ]

    def export_to_json(self, status: FileStatus, output_path: Path) -> int:
        matching = {
            fid: _file_progress_to_dict(fp)
            for fid, fp in self._files.items()
            if fp.status == status
        }
        output_path.write_text(json.dumps(matching, indent=2) + "\n", encoding="utf-8")
        return len(matching)

    def _build_summary_dict(self) -> Dict[str, int]:
        summary = self.get_summary()
        return {
            "pending": summary.pending,
            "downloaded": summary.downloaded,
            "uploaded": summary.uploaded,
            "completed": summary.completed,
            "failed": summary.failed,
            "orphan": summary.orphan,
            "needs_review": summary.needs_review,
            "partial": summary.partial,
        }
