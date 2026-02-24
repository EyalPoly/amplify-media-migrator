import json
import shutil
from pathlib import Path

import pytest

from amplify_media_migrator.migration.progress import (
    FileProgress,
    FileStatus,
    ProgressSummary,
    ProgressTracker,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def tracker(tmp_path):  # type: ignore[no-untyped-def]
    return ProgressTracker(progress_dir=tmp_path)


class TestProgressTrackerInit:
    def test_no_folder_id(self, tracker: ProgressTracker) -> None:
        assert tracker.folder_id is None
        assert tracker.progress_path is None
        assert tracker.total_files == 0

    def test_load_creates_new_progress(self, tracker: ProgressTracker) -> None:
        loaded = tracker.load("test_folder")
        assert loaded is False
        assert tracker.folder_id == "test_folder"
        assert tracker.total_files == 0

    def test_progress_path(self, tracker: ProgressTracker, tmp_path) -> None:  # type: ignore[no-untyped-def]
        tracker.load("abc123")
        assert tracker.progress_path == tmp_path / "progress_abc123.json"


class TestProgressTrackerSaveLoad:
    def test_save_and_reload(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.set_total_files(100)
        tracker.update_file(
            "f1", "12345.jpg", FileStatus.COMPLETED, sequential_ids=[12345]
        )
        tracker.save()

        tracker2 = ProgressTracker(progress_dir=tracker._progress_dir)
        loaded = tracker2.load("folder1")
        assert loaded is True
        assert tracker2.total_files == 100
        f1 = tracker2.get_file("f1")
        assert f1 is not None
        assert f1.filename == "12345.jpg"
        assert f1.status == FileStatus.COMPLETED
        assert f1.sequential_ids == [12345]

    def test_save_without_folder_id_raises(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        tracker = ProgressTracker(progress_dir=tmp_path)
        with pytest.raises(RuntimeError, match="no folder_id"):
            tracker.save()

    def test_load_corrupt_json(self, tracker: ProgressTracker, tmp_path) -> None:  # type: ignore[no-untyped-def]
        tracker.load("corrupt")
        path = tmp_path / "progress_corrupt.json"
        path.write_text("not json!", encoding="utf-8")
        loaded = tracker.load("corrupt")
        assert loaded is False
        assert len(tracker.files) == 0


class TestUpdateFile:
    def test_create_new_file(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "100.jpg", FileStatus.PENDING, sequential_ids=[100])
        f = tracker.get_file("f1")
        assert f is not None
        assert f.filename == "100.jpg"
        assert f.status == FileStatus.PENDING
        assert f.sequential_ids == [100]
        assert f.updated_at is not None

    def test_update_existing_file(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "100.jpg", FileStatus.PENDING)
        tracker.update_file(
            "f1",
            "100.jpg",
            FileStatus.COMPLETED,
            observation_ids=["obs-1"],
            s3_url="https://bucket/media/obs-1/100.jpg",
        )
        f = tracker.get_file("f1")
        assert f is not None
        assert f.status == FileStatus.COMPLETED
        assert f.observation_ids == ["obs-1"]
        assert f.s3_url == "https://bucket/media/obs-1/100.jpg"

    def test_update_preserves_existing_fields(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "100.jpg", FileStatus.PENDING, sequential_ids=[100])
        tracker.update_file("f1", "100.jpg", FileStatus.DOWNLOADED)
        f = tracker.get_file("f1")
        assert f is not None
        assert f.sequential_ids == [100]


class TestGetFilesByStatus:
    def test_filter(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.COMPLETED)
        tracker.update_file("f2", "2.jpg", FileStatus.FAILED, error="timeout")
        tracker.update_file("f3", "3.jpg", FileStatus.COMPLETED)
        tracker.update_file("f4", "4.jpg", FileStatus.NEEDS_REVIEW, error="bad name")

        completed = tracker.get_files_by_status(FileStatus.COMPLETED)
        assert len(completed) == 2
        failed = tracker.get_files_by_status(FileStatus.FAILED)
        assert len(failed) == 1
        assert failed[0].error == "timeout"
        review = tracker.get_files_by_status(FileStatus.NEEDS_REVIEW)
        assert len(review) == 1


class TestSummary:
    def test_empty(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        summary = tracker.get_summary()
        assert summary.completed == 0
        assert summary.failed == 0

    def test_counts(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.COMPLETED)
        tracker.update_file("f2", "2.jpg", FileStatus.COMPLETED)
        tracker.update_file("f3", "3.jpg", FileStatus.FAILED)
        tracker.update_file("f4", "4.jpg", FileStatus.ORPHAN)
        tracker.update_file("f5", "5.jpg", FileStatus.NEEDS_REVIEW)
        tracker.update_file("f6", "6.jpg", FileStatus.PARTIAL)

        summary = tracker.get_summary()
        assert summary.completed == 2
        assert summary.failed == 1
        assert summary.orphan == 1
        assert summary.needs_review == 1
        assert summary.partial == 1


class TestPendingAndFailedIds:
    def test_pending_ids(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.PENDING)
        tracker.update_file("f2", "2.jpg", FileStatus.COMPLETED)
        tracker.update_file("f3", "3.jpg", FileStatus.PENDING)

        ids = tracker.get_pending_file_ids()
        assert sorted(ids) == ["f1", "f3"]

    def test_failed_ids(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.FAILED)
        tracker.update_file("f2", "2.jpg", FileStatus.COMPLETED)

        ids = tracker.get_failed_file_ids()
        assert ids == ["f1"]


class TestPartialFileIds:
    def test_partial_ids(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.PARTIAL)
        tracker.update_file("f2", "2.jpg", FileStatus.COMPLETED)
        tracker.update_file("f3", "3.jpg", FileStatus.PARTIAL)

        ids = tracker.get_partial_file_ids()
        assert sorted(ids) == ["f1", "f3"]

    def test_partial_ids_empty(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.COMPLETED)

        ids = tracker.get_partial_file_ids()
        assert ids == []


class TestDownloadedUploadedStatuses:
    def test_downloaded_in_summary(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.DOWNLOADED)
        tracker.update_file("f2", "2.jpg", FileStatus.UPLOADED)

        summary = tracker.get_summary()
        assert summary.downloaded == 1
        assert summary.uploaded == 1

    def test_all_statuses_in_summary(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.PENDING)
        tracker.update_file("f2", "2.jpg", FileStatus.DOWNLOADED)
        tracker.update_file("f3", "3.jpg", FileStatus.UPLOADED)
        tracker.update_file("f4", "4.jpg", FileStatus.COMPLETED)
        tracker.update_file("f5", "5.jpg", FileStatus.FAILED)
        tracker.update_file("f6", "6.jpg", FileStatus.ORPHAN)
        tracker.update_file("f7", "7.jpg", FileStatus.NEEDS_REVIEW)
        tracker.update_file("f8", "8.jpg", FileStatus.PARTIAL)

        summary = tracker.get_summary()
        assert summary.pending == 1
        assert summary.downloaded == 1
        assert summary.uploaded == 1
        assert summary.completed == 1
        assert summary.failed == 1
        assert summary.orphan == 1
        assert summary.needs_review == 1
        assert summary.partial == 1


class TestFullFieldSerialization:
    def test_all_fields_roundtrip(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file(
            "f1",
            "6000-6001.jpg",
            FileStatus.COMPLETED,
            sequential_ids=[6000, 6001],
            observation_ids=["obs-a", "obs-b"],
            s3_url="https://bucket/media/obs-a/6000-6001.jpg",
            media_ids=["media-1", "media-2"],
        )
        tracker.save()

        tracker2 = ProgressTracker(progress_dir=tracker._progress_dir)
        tracker2.load("folder1")
        f1 = tracker2.get_file("f1")
        assert f1 is not None
        assert f1.filename == "6000-6001.jpg"
        assert f1.status == FileStatus.COMPLETED
        assert f1.sequential_ids == [6000, 6001]
        assert f1.observation_ids == ["obs-a", "obs-b"]
        assert f1.s3_url == "https://bucket/media/obs-a/6000-6001.jpg"
        assert f1.media_ids == ["media-1", "media-2"]
        assert f1.error is None
        assert f1.updated_at is not None

    def test_error_field_roundtrip(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file(
            "f1",
            "bad.txt",
            FileStatus.NEEDS_REVIEW,
            error="Unsupported extension",
        )
        tracker.save()

        tracker2 = ProgressTracker(progress_dir=tracker._progress_dir)
        tracker2.load("folder1")
        f1 = tracker2.get_file("f1")
        assert f1 is not None
        assert f1.error == "Unsupported extension"


class TestBuildSummaryDict:
    def test_summary_dict_keys(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.COMPLETED)
        tracker.update_file("f2", "2.jpg", FileStatus.FAILED)

        summary_dict = tracker._build_summary_dict()
        assert set(summary_dict.keys()) == {
            "pending",
            "downloaded",
            "uploaded",
            "completed",
            "failed",
            "orphan",
            "needs_review",
            "partial",
        }
        assert summary_dict["completed"] == 1
        assert summary_dict["failed"] == 1

    def test_summary_saved_in_progress_file(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.COMPLETED)
        tracker.save()

        path = tracker.progress_path
        assert path is not None
        data = json.loads(path.read_text())
        assert "summary" in data
        assert data["summary"]["completed"] == 1


class TestUpdateFileClearsError:
    def test_error_cleared_on_status_update(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.FAILED, error="old error")
        tracker.update_file("f1", "1.jpg", FileStatus.COMPLETED)
        f = tracker.get_file("f1")
        assert f is not None
        assert f.error is None

    def test_media_ids_updated(self, tracker: ProgressTracker) -> None:
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.PENDING)
        tracker.update_file(
            "f1", "1.jpg", FileStatus.COMPLETED, media_ids=["m-1", "m-2"]
        )
        f = tracker.get_file("f1")
        assert f is not None
        assert f.media_ids == ["m-1", "m-2"]


class TestExportToJson:
    def test_export(self, tracker: ProgressTracker, tmp_path) -> None:  # type: ignore[no-untyped-def]
        tracker.load("folder1")
        tracker.update_file("f1", "1.jpg", FileStatus.NEEDS_REVIEW, error="bad pattern")
        tracker.update_file("f2", "2.jpg", FileStatus.COMPLETED)
        tracker.update_file("f3", "3.jpg", FileStatus.NEEDS_REVIEW, error="no ext")

        output = tmp_path / "export.json"
        count = tracker.export_to_json(FileStatus.NEEDS_REVIEW, output)
        assert count == 2

        data = json.loads(output.read_text())
        assert "f1" in data
        assert "f3" in data
        assert "f2" not in data

    def test_export_empty(self, tracker: ProgressTracker, tmp_path) -> None:  # type: ignore[no-untyped-def]
        tracker.load("folder1")
        output = tmp_path / "export.json"
        count = tracker.export_to_json(FileStatus.ORPHAN, output)
        assert count == 0


class TestLoadFromFixture:
    def test_load_sample_progress_file(self, tmp_path) -> None:  # type: ignore[no-untyped-def]
        fixture_path = FIXTURES_DIR / "sample_progress_file.json"
        dest = tmp_path / "progress_1ABC_test_folder.json"
        shutil.copy(fixture_path, dest)

        tracker = ProgressTracker(progress_dir=tmp_path)
        loaded = tracker.load("1ABC_test_folder")
        assert loaded is True
        assert tracker.total_files == 9

        summary = tracker.get_summary()
        assert summary.completed == 2
        assert summary.failed == 1
        assert summary.orphan == 1
        assert summary.needs_review == 1
        assert summary.partial == 1
        assert summary.pending == 1
        assert summary.downloaded == 1
        assert summary.uploaded == 1

        completed = tracker.get_file("file-single-1")
        assert completed is not None
        assert completed.status == FileStatus.COMPLETED
        assert completed.media_ids == ["media-xyz-456"]

        partial = tracker.get_file("file-partial-1")
        assert partial is not None
        assert partial.status == FileStatus.PARTIAL
        assert partial.sequential_ids == [5000, 5001, 5002]

        pending = tracker.get_pending_file_ids()
        assert pending == ["file-pending-1"]

        failed = tracker.get_failed_file_ids()
        assert failed == ["file-failed-1"]

        partial_ids = tracker.get_partial_file_ids()
        assert partial_ids == ["file-partial-1"]
