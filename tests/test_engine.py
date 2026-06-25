import asyncio
from typing import Dict, List, Optional
from unittest.mock import ANY, MagicMock, patch

import pytest

from amplify_media_migrator.migration.concurrency import AdaptiveSettings
from amplify_media_migrator.migration.engine import MigrationEngine
from amplify_media_migrator.migration.mapper import (
    FilenameMapper,
    FilenamePattern,
    ParsedFilename,
)
from amplify_media_migrator.migration.progress import (
    FileProgress,
    FileStatus,
    ProgressTracker,
)
from amplify_media_migrator.sources.google_drive import DriveFile, GoogleDriveClient
from amplify_media_migrator.targets.graphql_client import (
    GraphQLClient,
    Observation,
    Media,
)
from amplify_media_migrator.targets.amplify_storage import AmplifyStorageClient
from amplify_media_migrator.utils.exceptions import (
    AuthenticationError,
    DownloadError,
    GraphQLError,
    MigratorError,
    RateLimitError,
    UploadError,
)
from amplify_media_migrator.utils.media import MediaType

pytestmark = pytest.mark.unit


def _drive_file(
    file_id: str = "file-1",
    name: str = "6602.jpg",
    mime_type: str = "image/jpeg",
    size: int = 1024,
) -> DriveFile:
    return DriveFile(id=file_id, name=name, mime_type=mime_type, size=size)


def _observation(obs_id: str = "obs-1", seq_id: int = 6602) -> Observation:
    return Observation(id=obs_id, sequential_id=seq_id)


def _media(
    media_id: str = "media-1",
    url: str = "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.jpg",
    obs_id: str = "obs-1",
) -> Media:
    return Media(
        id=media_id,
        url=url,
        observation_id=obs_id,
        type=MediaType.IMAGE,
        is_available_for_public_use=False,
    )


@pytest.fixture
def drive_client() -> MagicMock:
    return MagicMock(spec=GoogleDriveClient)


@pytest.fixture
def storage_client() -> MagicMock:
    mock = MagicMock(spec=AmplifyStorageClient)
    mock.get_url.side_effect = (
        lambda key: f"https://bucket.s3.us-east-1.amazonaws.com/{key}"
    )
    return mock


@pytest.fixture
def graphql_client() -> MagicMock:
    mock = MagicMock(spec=GraphQLClient)
    mock.get_media_by_url.return_value = None

    def _lookup_single(sequential_id: int) -> Optional[Observation]:
        batch = mock.get_observations_by_sequential_ids.return_value
        if isinstance(batch, dict):
            return batch.get(sequential_id)
        return None

    mock.get_observation_by_sequential_id.side_effect = _lookup_single
    return mock


@pytest.fixture
def progress(tmp_path: object) -> ProgressTracker:
    return ProgressTracker(progress_dir=tmp_path)  # type: ignore[arg-type]


@pytest.fixture
def mapper() -> FilenameMapper:
    return FilenameMapper()


@pytest.fixture
def engine(
    drive_client: MagicMock,
    storage_client: MagicMock,
    graphql_client: MagicMock,
    progress: ProgressTracker,
    mapper: FilenameMapper,
) -> MigrationEngine:
    return MigrationEngine(
        drive_client=drive_client,
        storage_client=storage_client,
        graphql_client=graphql_client,
        progress_tracker=progress,
        mapper=mapper,
        concurrency=2,
        retry_attempts=2,
        retry_delay_seconds=0,
    )


class TestScan:
    def test_registers_valid_files(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [
            _drive_file("f1", "6602.jpg"),
            _drive_file("f2", "6603a.jpg"),
        ]
        result = asyncio.run(engine.scan("folder-1"))

        assert result["single"] == 1
        assert result["multiple"] == 1
        assert progress.total_files == 2
        assert progress.files["f1"].status == FileStatus.PENDING
        assert progress.files["f2"].status == FileStatus.PENDING

    def test_registers_invalid_files_as_needs_review(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [
            _drive_file("f1", "bad.txt"),
        ]
        result = asyncio.run(engine.scan("folder-1"))

        assert result["invalid"] == 1
        assert progress.files["f1"].status == FileStatus.NEEDS_REVIEW

    def test_registers_range_files(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [
            _drive_file("f1", "6000-6001.jpg"),
        ]
        result = asyncio.run(engine.scan("folder-1"))

        assert result["range"] == 1
        assert progress.files["f1"].sequential_ids == [6000, 6001]

    def test_does_not_overwrite_existing_progress(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [
            _drive_file("f1", "6602.jpg"),
        ]
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="6602.jpg",
            status=FileStatus.COMPLETED,
        )
        progress.save()
        asyncio.run(engine.scan("folder-1"))

        assert progress.files["f1"].status == FileStatus.COMPLETED


class TestProcessFileSingle:
    def test_full_pipeline(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        media = _media("media-1")

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"photo bytes"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.jpg"
        )
        graphql_client.create_media.return_value = media

        asyncio.run(engine.process_file(file))

        drive_client.download_file.assert_called_once_with("f1", ANY)
        storage_client.upload_file.assert_called_once_with(
            b"photo bytes", "media/obs-1/6602.jpg", "image/jpeg", ANY
        )
        graphql_client.create_media.assert_called_once_with(
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.jpg",
            "obs-1",
            MediaType.IMAGE,
            False,
        )

        fp = progress.files["f1"]
        assert fp.status == FileStatus.COMPLETED
        assert (
            fp.s3_url
            == "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.jpg"
        )
        assert fp.media_ids == ["media-1"]
        assert fp.observation_ids == ["obs-1"]

    def test_video_file(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.mp4", "video/mp4")
        obs = _observation("obs-1", 6602)
        media = _media("media-1")

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"video bytes"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.mp4"
        )
        graphql_client.create_media.return_value = media

        asyncio.run(engine.process_file(file))

        graphql_client.create_media.assert_called_once_with(
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.mp4",
            "obs-1",
            MediaType.VIDEO,
            False,
        )


class TestProcessFileMultiple:
    def test_multiple_pattern(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602a.jpg")
        obs = _observation("obs-1", 6602)
        media = _media("media-1")

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"photo bytes"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602a.jpg"
        )
        graphql_client.create_media.return_value = media

        asyncio.run(engine.process_file(file))

        storage_client.upload_file.assert_called_once_with(
            b"photo bytes", "media/obs-1/6602a.jpg", "image/jpeg", ANY
        )
        assert progress.files["f1"].status == FileStatus.COMPLETED


class TestProcessFileRange:
    def test_creates_media_for_each_observation(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6000-6001.jpg")
        obs_a = _observation("obs-a", 6000)
        obs_b = _observation("obs-b", 6001)

        graphql_client.get_observations_by_sequential_ids.return_value = {
            6000: obs_a,
            6001: obs_b,
        }
        drive_client.download_file.return_value = b"photo"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-a/6000-6001.jpg"
        )
        graphql_client.create_media.side_effect = [
            _media("m-a", obs_id="obs-a"),
            _media("m-b", obs_id="obs-b"),
        ]

        asyncio.run(engine.process_file(file))

        assert graphql_client.create_media.call_count == 2
        storage_client.upload_file.assert_called_once()

        fp = progress.files["f1"]
        assert fp.status == FileStatus.COMPLETED
        assert set(fp.media_ids) == {"m-a", "m-b"}
        assert set(fp.observation_ids) == {"obs-a", "obs-b"}

    def test_partial_when_some_observations_fail(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6000-6001.jpg")
        obs_a = _observation("obs-a", 6000)
        obs_b = _observation("obs-b", 6001)

        graphql_client.get_observations_by_sequential_ids.return_value = {
            6000: obs_a,
            6001: obs_b,
        }
        drive_client.download_file.return_value = b"photo"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-a/6000-6001.jpg"
        )
        graphql_client.create_media.side_effect = [
            _media("m-a", obs_id="obs-a"),
            GraphQLError("Server error", operation="CreateMedia"),
        ]

        asyncio.run(engine.process_file(file))

        fp = progress.files["f1"]
        assert fp.status == FileStatus.PARTIAL
        assert fp.media_ids == ["m-a"]

    def test_lookups_run_concurrently(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        import threading

        progress.load("folder-1")
        file = _drive_file("f1", "6000-6001.jpg")

        in_flight = threading.Barrier(2, timeout=2.0)
        max_concurrent = {"value": 0}
        lock = threading.Lock()
        active = {"value": 0}

        def slow_lookup(seq_id: int) -> Observation:
            with lock:
                active["value"] += 1
                max_concurrent["value"] = max(max_concurrent["value"], active["value"])
            in_flight.wait()
            with lock:
                active["value"] -= 1
            return _observation(f"obs-{seq_id}", seq_id)

        graphql_client.get_observation_by_sequential_id.side_effect = slow_lookup
        drive_client.download_file.return_value = b"photo"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-6000/6000-6001.jpg"
        )
        graphql_client.create_media.side_effect = [
            _media("m-a", obs_id="obs-6000"),
            _media("m-b", obs_id="obs-6001"),
        ]

        asyncio.run(engine.process_file(file))

        assert max_concurrent["value"] == 2

    def test_some_observations_not_found(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6000-6001.jpg")
        obs_a = _observation("obs-a", 6000)

        graphql_client.get_observations_by_sequential_ids.return_value = {6000: obs_a}
        drive_client.download_file.return_value = b"photo"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-a/6000-6001.jpg"
        )
        graphql_client.create_media.return_value = _media("m-a", obs_id="obs-a")

        asyncio.run(engine.process_file(file))

        fp = progress.files["f1"]
        assert fp.status == FileStatus.COMPLETED
        assert fp.media_ids == ["m-a"]


class TestProcessFileInvalid:
    def test_marks_needs_review(
        self,
        engine: MigrationEngine,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "bad_name.txt")

        asyncio.run(engine.process_file(file))

        assert progress.files["f1"].status == FileStatus.NEEDS_REVIEW


class TestProcessFileOrphan:
    def test_marks_orphan_when_no_observations(
        self,
        engine: MigrationEngine,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "99999.jpg")
        graphql_client.get_observations_by_sequential_ids.return_value = {}

        asyncio.run(engine.process_file(file))

        fp = progress.files["f1"]
        assert fp.status == FileStatus.ORPHAN
        assert "No matching observations" in (fp.error or "")


class TestDryRun:
    def test_skips_download_and_upload(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}

        asyncio.run(engine.process_file(file, dry_run=True))

        drive_client.download_file.assert_not_called()
        storage_client.upload_file.assert_not_called()
        graphql_client.create_media.assert_not_called()

        assert "f1" not in progress.files


class TestDuplicateCheck:
    def test_skips_when_media_exists(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        graphql_client.get_media_by_url.return_value = _media("existing-m")

        asyncio.run(engine.process_file(file))

        drive_client.download_file.assert_not_called()
        assert progress.files["f1"].status == FileStatus.COMPLETED

    def test_proceeds_when_no_existing_media(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        graphql_client.get_media_by_url.return_value = None
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.jpg"
        )
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        drive_client.download_file.assert_called_once()
        assert progress.files["f1"].status == FileStatus.COMPLETED


class TestUrlCache:
    def test_populate_cache_from_completed_files(
        self,
        engine: MigrationEngine,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            "f1", "6602.jpg", FileStatus.COMPLETED, s3_url="https://u/1"
        )
        progress.update_file("f2", "6603.jpg", FileStatus.PENDING)
        progress.update_file(
            "f3", "6604.jpg", FileStatus.UPLOADED, s3_url="https://u/3"
        )
        progress.update_file("f4", "6605.jpg", FileStatus.COMPLETED)

        engine._populate_url_cache()

        assert engine._uploaded_urls == {"https://u/1"}

    def test_skips_duplicate_check_when_url_cached(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        engine._uploaded_urls.add(
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.jpg"
        )

        asyncio.run(engine.process_file(file))

        graphql_client.get_media_by_url.assert_not_called()
        drive_client.download_file.assert_not_called()
        fp = progress.files["f1"]
        assert fp.status == FileStatus.COMPLETED
        assert fp.observation_ids == ["obs-1"]
        assert (
            fp.s3_url
            == "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.jpg"
        )

    def test_calls_duplicate_check_when_url_not_cached(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        graphql_client.get_media_by_url.return_value = None
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.jpg"
        )
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        graphql_client.get_media_by_url.assert_called_once()

    def test_adds_url_to_cache_after_completion(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        url = "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/6602.jpg"
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        graphql_client.get_media_by_url.return_value = None
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = url
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        assert url in engine._uploaded_urls


class TestErrorHandling:
    def test_download_error_marks_failed(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.side_effect = DownloadError("Network error")

        asyncio.run(engine.process_file(file))

        assert progress.files["f1"].status == FileStatus.FAILED
        assert "Download failed" in (progress.files["f1"].error or "")

    def test_upload_error_marks_failed(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.side_effect = UploadError("S3 error")

        asyncio.run(engine.process_file(file))

        assert progress.files["f1"].status == FileStatus.FAILED
        assert "Upload failed" in (progress.files["f1"].error or "")

    def test_observation_query_error_marks_failed(
        self,
        engine: MigrationEngine,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")

        graphql_client.get_observation_by_sequential_id.side_effect = GraphQLError(
            "Server error", operation="query"
        )

        asyncio.run(engine.process_file(file))

        assert progress.files["f1"].status == FileStatus.FAILED
        assert "Observation query failed" in (progress.files["f1"].error or "")

    def test_auth_error_propagates(
        self,
        engine: MigrationEngine,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")

        graphql_client.get_observation_by_sequential_id.side_effect = (
            AuthenticationError("Token expired", provider="cognito")
        )

        with pytest.raises(AuthenticationError):
            asyncio.run(engine.process_file(file))

    def test_auth_error_during_download_propagates(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.side_effect = AuthenticationError(
            "Expired", provider="google_drive"
        )

        with pytest.raises(AuthenticationError):
            asyncio.run(engine.process_file(file))

    def test_auth_error_during_upload_propagates(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.side_effect = AuthenticationError(
            "Expired", provider="cognito"
        )

        with pytest.raises(AuthenticationError):
            asyncio.run(engine.process_file(file))

    def test_auth_error_during_create_media_propagates(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.side_effect = AuthenticationError(
            "Expired", provider="cognito"
        )

        with pytest.raises(AuthenticationError):
            asyncio.run(engine.process_file(file))

    def test_all_media_creation_fails(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.side_effect = GraphQLError(
            "Server error", operation="CreateMedia"
        )

        asyncio.run(engine.process_file(file))

        fp = progress.files["f1"]
        assert fp.status == FileStatus.FAILED
        assert "Failed to create any Media records" in (fp.error or "")


class TestRetry:
    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_retries_on_download_error(
        self,
        _mock_random: MagicMock,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.side_effect = [
            DownloadError("Transient"),
            b"photo bytes",
        ]
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        assert drive_client.download_file.call_count == 2
        assert progress.files["f1"].status == FileStatus.COMPLETED

    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_retries_on_rate_limit(
        self,
        _mock_random: MagicMock,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.side_effect = [
            RateLimitError("429", retry_after=0.0),
            b"data",
        ]
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        assert drive_client.download_file.call_count == 2
        assert progress.files["f1"].status == FileStatus.COMPLETED

    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_exhausted_retries_marks_failed(
        self,
        _mock_random: MagicMock,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.side_effect = DownloadError("Persistent")

        asyncio.run(engine.process_file(file))

        assert drive_client.download_file.call_count == 2
        assert progress.files["f1"].status == FileStatus.FAILED

    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_retries_upload_on_connection_error(
        self,
        _mock_random: MagicMock,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"photo bytes"
        storage_client.upload_file.side_effect = [
            UploadError("S3 connection error: could not resolve host"),
            "https://bucket/media/obs-1/6602.jpg",
        ]
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        assert storage_client.upload_file.call_count == 2
        assert progress.files["f1"].status == FileStatus.COMPLETED

    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_upload_exhausts_retries_then_fails(
        self,
        _mock_random: MagicMock,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"photo bytes"
        storage_client.upload_file.side_effect = UploadError("S3 connection error")

        asyncio.run(engine.process_file(file))

        assert storage_client.upload_file.call_count == 2
        assert progress.files["f1"].status == FileStatus.FAILED
        graphql_client.create_media.assert_not_called()

    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_retries_create_media_on_transient_graphql_error(
        self,
        _mock_random: MagicMock,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.side_effect = [
            GraphQLError(
                "Connection reset by peer",
                operation="CreateMedia",
                is_retryable=True,
            ),
            _media("m-1"),
        ]

        asyncio.run(engine.process_file(file))

        assert graphql_client.create_media.call_count == 2
        assert progress.files["f1"].status == FileStatus.COMPLETED

    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_does_not_retry_create_media_on_non_retryable_error(
        self,
        _mock_random: MagicMock,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.side_effect = GraphQLError(
            "Schema validation failed", operation="CreateMedia"
        )

        asyncio.run(engine.process_file(file))

        assert graphql_client.create_media.call_count == 1
        assert progress.files["f1"].status == FileStatus.FAILED

    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_create_media_exhausts_retries_then_fails(
        self,
        _mock_random: MagicMock,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.side_effect = GraphQLError(
            "Connection reset by peer",
            operation="CreateMedia",
            is_retryable=True,
        )

        asyncio.run(engine.process_file(file))

        assert graphql_client.create_media.call_count == 2
        assert progress.files["f1"].status == FileStatus.FAILED


class TestTokenExpiryRecovery:
    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_stream_upload_forces_refresh_on_expired_token(
        self,
        _mock_random: MagicMock,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
        mapper: FilenameMapper,
    ) -> None:
        token_manager = MagicMock()
        token_manager.force_refresh.return_value = True

        engine = MigrationEngine(
            drive_client=drive_client,
            storage_client=storage_client,
            graphql_client=graphql_client,
            progress_tracker=progress,
            mapper=mapper,
            concurrency=2,
            retry_attempts=2,
            retry_delay_seconds=0,
            token_manager=token_manager,
        )

        progress.load("folder-1")
        file = _drive_file("f1", "789.jpg", size=0)
        graphql_client.get_observations_by_sequential_ids.return_value = {
            789: _observation("obs-1", 789)
        }
        drive_client.open_download_stream.return_value = MagicMock()
        storage_client.upload_file_stream.side_effect = [
            UploadError("token expired", is_token_expired=True),
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/789.jpg",
        ]
        graphql_client.create_media.return_value = _media(
            "m-1",
            url="https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/789.jpg",
        )

        asyncio.run(engine.process_file(file))

        token_manager.force_refresh.assert_called_once()
        assert storage_client.upload_file_stream.call_count == 2
        assert progress.files["f1"].status == FileStatus.COMPLETED

    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_expired_token_without_token_manager_falls_back_to_backoff(
        self,
        _mock_random: MagicMock,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "789.jpg", size=0)
        graphql_client.get_observations_by_sequential_ids.return_value = {
            789: _observation("obs-1", 789)
        }
        drive_client.open_download_stream.return_value = MagicMock()
        storage_client.upload_file_stream.side_effect = UploadError(
            "token expired", is_token_expired=True
        )

        asyncio.run(engine.process_file(file))

        assert storage_client.upload_file_stream.call_count == 2
        assert progress.files["f1"].status == FileStatus.FAILED

    @patch("amplify_media_migrator.migration.engine.random.uniform", return_value=0)
    def test_failed_refresh_falls_back_to_backoff(
        self,
        _mock_random: MagicMock,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
        mapper: FilenameMapper,
    ) -> None:
        token_manager = MagicMock()
        token_manager.force_refresh.return_value = False

        engine = MigrationEngine(
            drive_client=drive_client,
            storage_client=storage_client,
            graphql_client=graphql_client,
            progress_tracker=progress,
            mapper=mapper,
            concurrency=2,
            retry_attempts=2,
            retry_delay_seconds=0,
            token_manager=token_manager,
        )

        progress.load("folder-1")
        file = _drive_file("f1", "789.jpg", size=0)
        graphql_client.get_observations_by_sequential_ids.return_value = {
            789: _observation("obs-1", 789)
        }
        drive_client.open_download_stream.return_value = MagicMock()
        storage_client.upload_file_stream.side_effect = UploadError(
            "token expired", is_token_expired=True
        )

        asyncio.run(engine.process_file(file))

        assert token_manager.force_refresh.call_count == 2
        assert progress.files["f1"].status == FileStatus.FAILED


class TestMigrate:
    def test_processes_all_pending_files(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [
            _drive_file("f1", "6602.jpg"),
            _drive_file("f2", "6603.jpg"),
        ]
        obs1 = _observation("obs-1", 6602)
        obs2 = _observation("obs-2", 6603)
        graphql_client.get_observation_by_sequential_id.side_effect = lambda sid: {
            6602: obs1,
            6603: obs2,
        }.get(sid)
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.side_effect = [
            "https://bucket/media/obs-1/6602.jpg",
            "https://bucket/media/obs-2/6603.jpg",
        ]
        graphql_client.create_media.side_effect = [
            _media("m-1", obs_id="obs-1"),
            _media("m-2", obs_id="obs-2"),
        ]

        asyncio.run(engine.migrate("folder-1"))

        assert progress.files["f1"].status == FileStatus.COMPLETED
        assert progress.files["f2"].status == FileStatus.COMPLETED

    def test_existing_progress_skips_drive_scan_and_retries_failed(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="6602.jpg",
            status=FileStatus.FAILED,
            error="Previous error",
            sequential_ids=[6602],
        )
        progress.save()

        drive_client.get_file_metadata.return_value = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file_stream.return_value = (
            "https://bucket/media/obs-1/6602.jpg"
        )
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.migrate("folder-1"))

        assert progress.files["f1"].status == FileStatus.COMPLETED
        drive_client.list_files.assert_not_called()

    def test_rescan_lists_drive_and_picks_up_new_file(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="6602.jpg",
            status=FileStatus.COMPLETED,
        )
        progress.save()

        drive_client.list_files.return_value = [
            _drive_file("f1", "6602.jpg"),
            _drive_file("f2", "6603.jpg"),
        ]
        obs2 = _observation("obs-2", 6603)
        graphql_client.get_observations_by_sequential_ids.return_value = {6603: obs2}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-2/6603.jpg"
        graphql_client.create_media.return_value = _media("m-2", obs_id="obs-2")

        asyncio.run(engine.migrate("folder-1", rescan=True))

        drive_client.list_files.assert_called_once()
        assert progress.files["f2"].status == FileStatus.COMPLETED
        assert drive_client.download_file.call_count == 1

    def test_reprocesses_needs_review_files_after_rename(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="bad.txt",
            status=FileStatus.NEEDS_REVIEW,
            error="Invalid filename pattern",
        )
        progress.save()

        # Same file ID, but user renamed it to a valid name in Drive
        drive_client.list_files.return_value = [_drive_file("f1", "6602.jpg")]
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.migrate("folder-1", rescan=True))

        assert progress.files["f1"].status == FileStatus.COMPLETED

    def test_needs_review_stays_if_still_invalid_after_rename(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="bad.txt",
            status=FileStatus.NEEDS_REVIEW,
            error="Invalid filename pattern",
        )
        progress.save()

        # Still an invalid name after "rename"
        drive_client.list_files.return_value = [_drive_file("f1", "still_bad.pdf")]

        asyncio.run(engine.migrate("folder-1", rescan=True))

        assert progress.files["f1"].status == FileStatus.NEEDS_REVIEW

    def test_skips_already_completed_files(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="6602.jpg",
            status=FileStatus.COMPLETED,
        )
        progress.save()

        drive_client.list_files.return_value = [
            _drive_file("f1", "6602.jpg"),
            _drive_file("f2", "6603.jpg"),
        ]
        obs2 = _observation("obs-2", 6603)
        graphql_client.get_observations_by_sequential_ids.return_value = {6603: obs2}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-2/6603.jpg"
        graphql_client.create_media.return_value = _media("m-2", obs_id="obs-2")

        asyncio.run(engine.migrate("folder-1", rescan=True))

        assert drive_client.download_file.call_count == 1
        assert progress.files["f2"].status == FileStatus.COMPLETED


class TestMigrateFromProgress:
    def test_retries_failed_files(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="6602.jpg",
            status=FileStatus.FAILED,
            error="Previous error",
            sequential_ids=[6602],
        )
        progress.save()

        drive_client.get_file_metadata.return_value = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file_stream.return_value = (
            "https://bucket/media/obs-1/6602.jpg"
        )
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.migrate("folder-1"))

        assert progress.files["f1"].status == FileStatus.COMPLETED

    def test_retries_partial_files(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        """PARTIAL files should be retried during resume."""
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="6000-6001.jpg",
            status=FileStatus.PARTIAL,
            error="Failed for sequential IDs: [6001]",
            sequential_ids=[6000, 6001],
        )
        progress.save()

        drive_client.get_file_metadata.return_value = _drive_file("f1", "6000-6001.jpg")
        obs_a = _observation("obs-a", 6000)
        obs_b = _observation("obs-b", 6001)
        graphql_client.get_observations_by_sequential_ids.return_value = {
            6000: obs_a,
            6001: obs_b,
        }
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file_stream.return_value = (
            "https://bucket/media/obs-a/6000-6001.jpg"
        )
        graphql_client.create_media.side_effect = [
            _media("m-a", obs_id="obs-a"),
            _media("m-b", obs_id="obs-b"),
        ]

        asyncio.run(engine.migrate("folder-1"))

        fp = progress.files["f1"]
        assert fp.status == FileStatus.COMPLETED
        assert fp.error is None

    def test_no_files_to_process(
        self,
        engine: MigrationEngine,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="6602.jpg",
            status=FileStatus.COMPLETED,
        )
        progress.save()

        asyncio.run(engine.migrate("folder-1"))

    def test_reprocesses_needs_review_files_after_rename(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="bad.txt",
            status=FileStatus.NEEDS_REVIEW,
            error="Invalid filename pattern",
        )
        progress.save()

        # Drive now returns a valid filename for the same file ID
        drive_client.get_file_metadata.return_value = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.migrate("folder-1"))

        assert progress.files["f1"].status == FileStatus.COMPLETED

    def test_skips_needs_review_if_still_invalid(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="bad.txt",
            status=FileStatus.NEEDS_REVIEW,
            error="Invalid filename pattern",
        )
        progress.save()

        drive_client.get_file_metadata.return_value = _drive_file("f1", "still_bad.pdf")

        asyncio.run(engine.migrate("folder-1"))

        assert progress.files["f1"].status == FileStatus.NEEDS_REVIEW

    def test_needs_review_only_does_not_skip_early(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        """resume should not bail out early when only needs_review files exist."""
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="bad.txt",
            status=FileStatus.NEEDS_REVIEW,
            error="Invalid filename pattern",
        )
        progress.save()

        drive_client.get_file_metadata.return_value = _drive_file("f1", "still_bad.pdf")

        # Should complete without raising
        asyncio.run(engine.migrate("folder-1"))
        drive_client.get_file_metadata.assert_called_once()

    def test_retries_orphan_files_when_flag_set(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="144.jpg",
            status=FileStatus.ORPHAN,
            sequential_ids=[144],
            error="No matching observations found",
        )
        progress.save()

        drive_client.get_file_metadata.return_value = _drive_file("f1", "144.jpg")
        obs = _observation("obs-144", 144)
        graphql_client.get_observations_by_sequential_ids.return_value = {144: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file_stream.return_value = (
            "https://bucket/media/obs-144/144.jpg"
        )
        graphql_client.create_media.return_value = _media("m-1", obs_id="obs-144")

        asyncio.run(engine.migrate("folder-1", retry_orphans=True))

        assert progress.files["f1"].status == FileStatus.COMPLETED

    def test_skips_orphan_files_by_default(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="144.jpg",
            status=FileStatus.ORPHAN,
            sequential_ids=[144],
            error="No matching observations found",
        )
        progress.save()

        asyncio.run(engine.migrate("folder-1"))

        assert progress.files["f1"].status == FileStatus.ORPHAN
        drive_client.get_file_metadata.assert_not_called()

    def test_retry_orphans_stays_orphan_when_still_not_found(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="144.jpg",
            status=FileStatus.ORPHAN,
            sequential_ids=[144],
            error="No matching observations found",
        )
        progress.save()

        drive_client.get_file_metadata.return_value = _drive_file("f1", "144.jpg")
        graphql_client.get_observations_by_sequential_ids.return_value = {}

        asyncio.run(engine.migrate("folder-1", retry_orphans=True))

        assert progress.files["f1"].status == FileStatus.ORPHAN


class TestRunWorkers:
    def test_runs_all_files(
        self, engine: MigrationEngine, progress: ProgressTracker
    ) -> None:
        progress.load("folder-1")
        files = [_drive_file(f"f{i}", f"{6000 + i}.jpg") for i in range(20)]
        processed: list = []

        async def fake_process(file: DriveFile, dry_run: bool = False) -> None:
            processed.append(file.id)

        engine.process_file = fake_process  # type: ignore[method-assign]
        asyncio.run(engine._run_workers(files, dry_run=True))

        assert sorted(processed) == sorted(f.id for f in files)

    def test_caps_in_flight_at_concurrency(
        self, engine: MigrationEngine, progress: ProgressTracker
    ) -> None:
        # concurrency=2: never more than 2 files in flight at once.
        progress.load("folder-1")
        files = [_drive_file(f"f{i}", f"{6000 + i}.jpg") for i in range(10)]
        max_concurrent = {"value": 0}
        active = {"value": 0}

        async def fake_process(file: DriveFile, dry_run: bool = False) -> None:
            active["value"] += 1
            max_concurrent["value"] = max(max_concurrent["value"], active["value"])
            await asyncio.sleep(0.01)
            active["value"] -= 1

        engine.process_file = fake_process  # type: ignore[method-assign]
        asyncio.run(engine._run_workers(files, dry_run=True))

        assert max_concurrent["value"] == 2

    def test_auth_error_stops_pulling_new_files(
        self, engine: MigrationEngine, progress: ProgressTracker
    ) -> None:
        progress.load("folder-1")
        files = [_drive_file(f"f{i}", f"{6000 + i}.jpg") for i in range(20)]
        processed: list = []

        async def fake_process(file: DriveFile, dry_run: bool = False) -> None:
            processed.append(file.id)
            if file.id == "f0":
                raise AuthenticationError("token expired")

        engine.process_file = fake_process  # type: ignore[method-assign]

        with pytest.raises(AuthenticationError):
            asyncio.run(engine._run_workers(files, dry_run=True))

        assert len(processed) < len(files)


class FakeReporter:
    def __init__(self) -> None:
        self.events: List[tuple] = []

    def on_total(self, total_files: int, total_bytes: int) -> None:
        self.events.append(("total", total_files, total_bytes))

    def on_file_start(self, file_id: str, name: str, size: int, phase: str) -> None:
        self.events.append(("start", file_id, name, size, phase))

    def on_file_bytes(self, file_id: str, bytes_done: int) -> None:
        self.events.append(("bytes", file_id, bytes_done))

    def on_file_phase(self, file_id: str, phase: str) -> None:
        self.events.append(("phase", file_id, phase))

    def on_file_done(self, file_id: str, status: FileStatus) -> None:
        self.events.append(("done", file_id, status))

    def on_concurrency(self, limit: int) -> None:
        self.events.append(("concurrency", limit))


class TestProgressReporter:
    def test_done_emitted_on_completion(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        reporter = FakeReporter()
        engine.set_reporter(reporter)

        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        assert ("done", "f1", FileStatus.COMPLETED) in reporter.events

    def test_phase_sequence_for_in_memory_path(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        reporter = FakeReporter()
        engine.set_reporter(reporter)

        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        kinds = [
            (e[0], e[-1]) for e in reporter.events if e[0] in ("start", "phase", "done")
        ]
        assert kinds == [
            ("start", "querying"),
            ("phase", "downloading"),
            ("phase", "uploading"),
            ("phase", "linking"),
            ("done", FileStatus.COMPLETED),
        ]

    def test_done_emitted_on_orphan(
        self,
        engine: MigrationEngine,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        reporter = FakeReporter()
        engine.set_reporter(reporter)

        file = _drive_file("f1", "99999.jpg")
        graphql_client.get_observations_by_sequential_ids.return_value = {}

        asyncio.run(engine.process_file(file))

        assert ("start", "f1", "99999.jpg", file.size, "querying") in reporter.events
        assert ("done", "f1", FileStatus.ORPHAN) in reporter.events

    def test_start_before_done(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        reporter = FakeReporter()
        engine.set_reporter(reporter)

        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        assert reporter.events[0] == ("start", "f1", "6602.jpg", file.size, "querying")
        assert reporter.events[-1] == ("done", "f1", FileStatus.COMPLETED)

    def test_bytes_forwarded_during_transfer(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        reporter = FakeReporter()
        engine.set_reporter(reporter)

        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}

        def _download(file_id: str, on_bytes: object = None) -> bytes:
            if on_bytes is not None:
                on_bytes(5)
                on_bytes(10)
            return b"data"

        drive_client.download_file.side_effect = _download
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        byte_events = [e for e in reporter.events if e[0] == "bytes"]
        assert ("bytes", "f1", 5) in byte_events
        assert ("bytes", "f1", 10) in byte_events

    def test_on_total_emitted_on_migrate(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [
            _drive_file("f1", "6602.jpg", size=100),
            _drive_file("f2", "6603.jpg", size=200),
        ]
        graphql_client.get_observations_by_sequential_ids.return_value = {}

        reporter = FakeReporter()
        engine.set_reporter(reporter)

        asyncio.run(engine.migrate("folder-1"))

        assert ("total", 2, 300) in reporter.events

    def test_null_reporter_default_does_not_raise(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
    ) -> None:
        drive_client.list_files.return_value = [_drive_file("f1", "6602.jpg")]
        graphql_client.get_observations_by_sequential_ids.return_value = {}
        asyncio.run(engine.migrate("folder-1"))

    def test_start_emitted_for_each_file(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [
            _drive_file("f1", "6602.jpg"),
            _drive_file("f2", "6603.jpg"),
        ]
        graphql_client.get_observations_by_sequential_ids.return_value = {}

        reporter = FakeReporter()
        engine.set_reporter(reporter)

        asyncio.run(engine.migrate("folder-1"))

        started = sorted(e[2] for e in reporter.events if e[0] == "start")
        assert started == ["6602.jpg", "6603.jpg"]


class TestEdgeCases:
    def test_empty_folder_scan(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = []
        result = asyncio.run(engine.scan("folder-1"))

        assert sum(result.values()) == 0
        assert progress.total_files == 0

    def test_empty_folder_migrate(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
    ) -> None:
        drive_client.list_files.return_value = []
        asyncio.run(engine.migrate("folder-1"))

        summary = engine.get_summary()
        assert summary["total"] == 0
        assert summary["completed"] == 0

    def test_default_media_public_flag(
        self,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
        mapper: FilenameMapper,
    ) -> None:
        engine = MigrationEngine(
            drive_client=drive_client,
            storage_client=storage_client,
            graphql_client=graphql_client,
            progress_tracker=progress,
            mapper=mapper,
            concurrency=1,
            retry_attempts=1,
            retry_delay_seconds=0,
            default_media_public=True,
        )
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        graphql_client.create_media.assert_called_once_with(
            "https://bucket/media/obs-1/6602.jpg",
            "obs-1",
            MediaType.IMAGE,
            True,
        )

    def test_duplicate_check_query_error_falls_through(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        """When skip-existing check fails with non-auth error, proceed with migration."""
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        graphql_client.get_media_by_url.side_effect = GraphQLError(
            "Server error", operation="GetMediaByUrl"
        )
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        drive_client.download_file.assert_called_once()
        assert progress.files["f1"].status == FileStatus.COMPLETED

    def test_duplicate_check_auth_error_propagates(
        self,
        engine: MigrationEngine,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        graphql_client.get_media_by_url.side_effect = AuthenticationError(
            "Token expired", provider="cognito"
        )

        with pytest.raises(AuthenticationError):
            asyncio.run(engine.process_file(file))

    def test_large_range_creates_many_media_records(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "1000-1005.jpg")

        observations = {
            seq_id: _observation(f"obs-{seq_id}", seq_id)
            for seq_id in range(1000, 1006)
        }
        graphql_client.get_observations_by_sequential_ids.return_value = observations
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = (
            "https://bucket/media/obs-1000/1000-1005.jpg"
        )
        graphql_client.create_media.side_effect = [
            _media(f"m-{i}", obs_id=f"obs-{i}") for i in range(1000, 1006)
        ]

        asyncio.run(engine.process_file(file))

        assert graphql_client.create_media.call_count == 6
        storage_client.upload_file.assert_called_once()

        fp = progress.files["f1"]
        assert fp.status == FileStatus.COMPLETED
        assert len(fp.media_ids) == 6

    def test_range_all_observations_orphaned(
        self,
        engine: MigrationEngine,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "6000-6001.jpg")

        graphql_client.get_observations_by_sequential_ids.return_value = {}

        asyncio.run(engine.process_file(file))

        fp = progress.files["f1"]
        assert fp.status == FileStatus.ORPHAN

    def test_concurrent_processing(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        """Verify multiple files are processed with concurrency."""
        files = [_drive_file(f"f{i}", f"{6600 + i}.jpg") for i in range(5)]
        drive_client.list_files.return_value = files

        def mock_get_obs(seq_id: int) -> Observation:
            return _observation(f"obs-{seq_id}", seq_id)

        graphql_client.get_observation_by_sequential_id.side_effect = mock_get_obs
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.side_effect = [
            f"https://bucket/media/obs-{6600 + i}/{6600 + i}.jpg" for i in range(5)
        ]
        graphql_client.create_media.side_effect = [_media(f"m-{i}") for i in range(5)]

        asyncio.run(engine.migrate("folder-1"))

        assert len(progress.files) == 5
        for fp in progress.files.values():
            assert fp.status == FileStatus.COMPLETED

    def test_migrate_invalid_files_not_processed(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        storage_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        """Invalid files registered as needs_review, not sent through pipeline."""
        drive_client.list_files.return_value = [
            _drive_file("f1", "6602.jpg"),
            _drive_file("f2", "bad_file.pdf"),
        ]
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.migrate("folder-1"))

        assert progress.files["f1"].status == FileStatus.COMPLETED
        assert progress.files["f2"].status == FileStatus.NEEDS_REVIEW
        assert drive_client.download_file.call_count == 1

    def test_resume_skips_file_id_with_no_progress_entry(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        """File IDs with no stored progress entry are silently skipped."""
        progress.load("folder-1")
        progress.save()

        with patch.object(progress, "get_pending_file_ids", return_value=["ghost-id"]):
            asyncio.run(engine.migrate("folder-1"))

        assert "ghost-id" not in progress.files

    def test_case_insensitive_extension(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        """Engine handles case-insensitive extensions via mapper."""
        progress.load("folder-1")
        file = _drive_file("f1", "6602.JPG")
        obs = _observation("obs-1", 6602)

        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.JPG"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        assert progress.files["f1"].status == FileStatus.COMPLETED


class TestErrorClearing:
    def test_successful_retry_clears_error(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        """After a failed file succeeds on retry, error field should be None."""
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="6602.jpg",
            status=FileStatus.FAILED,
            error="Previous download error",
        )

        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.run(engine.process_file(file))

        fp = progress.files["f1"]
        assert fp.status == FileStatus.COMPLETED
        assert fp.error is None


class TestAutosave:
    def test_save_called_in_finally_on_completion(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [_drive_file("f1", "6602.jpg")]
        graphql_client.get_observations_by_sequential_ids.return_value = {
            6602: _observation("obs-1", 6602)
        }
        graphql_client.get_media_by_url.return_value = None
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        with patch.object(progress, "save", wraps=progress.save) as save_spy:
            asyncio.run(engine.migrate("folder-1"))

        # once after scan, once in finally
        assert save_spy.call_count >= 2

    def test_save_called_in_finally_on_keyboard_interrupt(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [_drive_file("f1", "6602.jpg")]

        # Raise KeyboardInterrupt from within the event loop (not a thread) so
        # it propagates cleanly through asyncio.gather and hits the finally block.
        async def _raise_ki(*args, **kwargs):  # type: ignore[no-untyped-def]
            raise KeyboardInterrupt()

        with patch.object(progress, "save", wraps=progress.save) as save_spy:
            with patch.object(engine, "process_file", side_effect=_raise_ki):
                with pytest.raises(KeyboardInterrupt):
                    asyncio.run(engine.migrate("folder-1"))

        assert save_spy.called

    def test_autosave_thread_fires_after_interval(
        self,
        engine: MigrationEngine,
    ) -> None:
        fired: list = []

        real_save = engine._progress.save

        def capturing_save() -> None:
            fired.append(1)
            real_save()

        engine._progress.save = capturing_save  # type: ignore[method-assign]

        # Run with a very short interval so it fires before the gather completes
        with patch.object(
            engine, "_start_autosave", wraps=engine._start_autosave
        ) as spy:
            stop = engine._start_autosave(interval=0.05)
            import time

            time.sleep(0.15)
            stop.set()

        assert len(fired) >= 1, "autosave thread should have fired at least once"

    def test_autosave_not_started_for_dry_run(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [_drive_file("f1", "6602.jpg")]
        graphql_client.get_observations_by_sequential_ids.return_value = {
            6602: _observation("obs-1", 6602)
        }
        graphql_client.get_media_by_url.return_value = None

        with patch.object(engine, "_start_autosave") as mock_autosave:
            asyncio.run(engine.migrate("folder-1", dry_run=True))

        mock_autosave.assert_not_called()

    def test_no_save_in_dry_run(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = [_drive_file("f1", "6602.jpg")]
        graphql_client.get_observations_by_sequential_ids.return_value = {
            6602: _observation("obs-1", 6602)
        }
        graphql_client.get_media_by_url.return_value = None

        with patch.object(progress, "save") as save_mock:
            asyncio.run(engine.migrate("folder-1", dry_run=True))

        save_mock.assert_not_called()


class TestGetSummary:
    def test_returns_summary_dict(
        self,
        engine: MigrationEngine,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        progress.set_total_files(3)
        progress.update_file("f1", "a.jpg", FileStatus.COMPLETED)
        progress.update_file("f2", "b.jpg", FileStatus.FAILED)
        progress.update_file("f3", "c.txt", FileStatus.NEEDS_REVIEW)

        summary = engine.get_summary()
        assert summary["total"] == 3
        assert summary["completed"] == 1
        assert summary["failed"] == 1
        assert summary["needs_review"] == 1
        assert summary["pending"] == 0


class TestProcessFileStreaming:
    """process_file uses the stream path for files above large_file_threshold_mb."""

    @pytest.fixture
    def streaming_engine(
        self,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
        mapper: FilenameMapper,
    ) -> MigrationEngine:
        return MigrationEngine(
            drive_client=drive_client,
            storage_client=storage_client,
            graphql_client=graphql_client,
            progress_tracker=progress,
            mapper=mapper,
            concurrency=2,
            retry_attempts=2,
            retry_delay_seconds=0,
            large_file_threshold_mb=25,
        )

    def test_small_file_uses_in_memory_path(
        self,
        streaming_engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f1", "123.jpg", size=10 * 1024 * 1024)
        graphql_client.get_observations_by_sequential_ids.return_value = {
            123: _observation("obs-1", 123)
        }
        drive_client.download_file.return_value = b"photo bytes"
        storage_client.upload_file.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-1/123.jpg"
        )
        graphql_client.create_media.return_value = _media("media-1")

        asyncio.run(streaming_engine.process_file(file))

        drive_client.download_file.assert_called_once_with("f1", ANY)
        drive_client.open_download_stream.assert_not_called()
        storage_client.upload_file_stream.assert_not_called()

    def test_large_file_uses_streaming_path(
        self,
        streaming_engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        file = _drive_file("f2", "456.mp4", "video/mp4", size=50 * 1024 * 1024)
        graphql_client.get_observations_by_sequential_ids.return_value = {
            456: _observation("obs-2", 456)
        }
        storage_client.open_download_stream = MagicMock()
        drive_client.open_download_stream.return_value = MagicMock()
        storage_client.upload_file_stream.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-2/456.mp4"
        )
        graphql_client.create_media.return_value = _media(
            "media-2",
            url="https://bucket.s3.us-east-1.amazonaws.com/media/obs-2/456.mp4",
            obs_id="obs-2",
        )

        asyncio.run(streaming_engine.process_file(file))

        drive_client.open_download_stream.assert_called_once_with("f2")
        storage_client.upload_file_stream.assert_called_once()
        storage_client.upload_file.assert_not_called()
        drive_client.download_file.assert_not_called()

        fp = progress.files["f2"]
        assert fp.status == FileStatus.COMPLETED
        assert (
            fp.s3_url == "https://bucket.s3.us-east-1.amazonaws.com/media/obs-2/456.mp4"
        )

    def test_unknown_size_uses_streaming_path(
        self,
        streaming_engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        # On resume, DriveFile is rebuilt from progress with size=0 (unknown).
        # Such files must stream (overlapping download+upload, bounded memory)
        # instead of buffering the whole file in RAM via download_file.
        progress.load("folder-1")
        file = _drive_file("f3", "789.jpg", size=0)
        graphql_client.get_observations_by_sequential_ids.return_value = {
            789: _observation("obs-3", 789)
        }
        drive_client.open_download_stream.return_value = MagicMock()
        storage_client.upload_file_stream.return_value = (
            "https://bucket.s3.us-east-1.amazonaws.com/media/obs-3/789.jpg"
        )
        graphql_client.create_media.return_value = _media(
            "media-3",
            url="https://bucket.s3.us-east-1.amazonaws.com/media/obs-3/789.jpg",
            obs_id="obs-3",
        )

        asyncio.run(streaming_engine.process_file(file))

        drive_client.open_download_stream.assert_called_once_with("f3")
        storage_client.upload_file_stream.assert_called_once()
        drive_client.download_file.assert_not_called()


class TestProcessFilesConcurrency:
    def test_slow_file_does_not_block_others(
        self,
        engine: MigrationEngine,
        progress: ProgressTracker,
    ) -> None:
        # A single stalled file must not block the others: with a worker pool,
        # the remaining workers keep pulling and processing files while f0 hangs.
        progress.load("folder-1")
        files = [_drive_file(f"f{i}", f"{6000 + i}.jpg") for i in range(9)]

        started: list = []

        async def run() -> None:
            release = asyncio.Event()

            async def fake_process(file: DriveFile, dry_run: bool = False) -> None:
                started.append(file.id)
                if file.id == "f0":
                    await release.wait()

            engine.process_file = fake_process  # type: ignore[method-assign]

            task = asyncio.create_task(engine._process_files(files, dry_run=True))
            for _ in range(100):
                await asyncio.sleep(0)
            await asyncio.sleep(0.05)

            assert "f8" in started, (
                f"f8 was never processed while f0 stalled; a single slow file "
                f"blocked the rest. started={started}"
            )

            release.set()
            await task

        asyncio.run(run())


class TestSessionCleanup:
    def test_migrate_closes_graphql_client(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
    ) -> None:
        drive_client.list_files.return_value = []

        asyncio.run(engine.migrate("folder-1"))

        graphql_client.close.assert_called_once_with()

    def test_migrate_closes_graphql_client_on_error(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        graphql_client: MagicMock,
    ) -> None:
        drive_client.list_files.return_value = [_drive_file("f1", "6602.jpg")]
        graphql_client.get_observation_by_sequential_id.side_effect = (
            AuthenticationError("token expired", provider="cognito")
        )

        with pytest.raises(AuthenticationError):
            asyncio.run(engine.migrate("folder-1"))

        graphql_client.close.assert_called_once_with()


class TestSelectByPrefix:
    PREFIXES = {"": "c-med", "E": "c-red", "S": "*"}

    def test_explicit_value_match(self):
        cands = [Observation("o1", 5, "c-med"), Observation("o2", 5, "c-red")]
        assert MigrationEngine._select_by_prefix(cands, "E", self.PREFIXES).id == "o2"

    def test_empty_prefix_matches_med(self):
        cands = [Observation("o1", 5, "c-med"), Observation("o2", 5, "c-red")]
        assert MigrationEngine._select_by_prefix(cands, "", self.PREFIXES).id == "o1"

    def test_catch_all_excludes_explicit_values(self):
        cands = [Observation("o1", 5, "c-med"), Observation("o3", 5, "c-egypt")]
        assert MigrationEngine._select_by_prefix(cands, "S", self.PREFIXES).id == "o3"

    def test_no_match_returns_none(self):
        cands = [Observation("o1", 5, "c-med")]
        assert MigrationEngine._select_by_prefix(cands, "E", self.PREFIXES) is None

    def test_unknown_prefix_returns_none(self):
        cands = [Observation("o1", 5, "c-med")]
        assert MigrationEngine._select_by_prefix(cands, "Z", self.PREFIXES) is None

    def test_ambiguous_raises(self):
        from amplify_media_migrator.utils.exceptions import MigratorError

        cands = [Observation("o3", 5, "c-egypt"), Observation("o4", 5, "c-jordan")]
        with pytest.raises(MigratorError):
            MigrationEngine._select_by_prefix(cands, "S", self.PREFIXES)


def _engine_basic(
    adaptive: bool = True,
    concurrency: int = 4,
    initial: Optional[int] = None,
    min_workers: int = 4,
) -> MigrationEngine:
    return MigrationEngine(
        drive_client=MagicMock(spec=GoogleDriveClient),
        storage_client=MagicMock(spec=AmplifyStorageClient),
        graphql_client=MagicMock(spec=GraphQLClient),
        progress_tracker=ProgressTracker(),
        mapper=FilenameMapper(),
        concurrency=concurrency,
        retry_attempts=1,
        retry_delay_seconds=0,
        adaptive=AdaptiveSettings(
            enabled=adaptive, min_workers=min_workers, initial_workers=initial
        ),
    )


def _engine_with_concurrency(initial: int, files: int) -> MigrationEngine:
    return _engine_basic(
        adaptive=True,
        concurrency=max(initial, 4),
        initial=initial,
        min_workers=min(initial, 4),
    )


class TestAdaptiveEngine:
    async def test_gate_caps_in_flight_workers(self) -> None:
        engine = _engine_with_concurrency(initial=2, files=8)
        assert engine._controller is not None
        assert engine._controller.current_limit() == 2

    def test_disabled_leaves_controller_none(self) -> None:
        engine = _engine_basic(adaptive=False)
        assert engine._controller is None

    def test_initial_workers_defaults_to_half_max(self) -> None:
        engine = _engine_basic(adaptive=True, concurrency=20, initial=None)
        assert engine._controller is not None
        assert engine._controller.current_limit() == 10

    async def test_retryable_media_error_is_recorded(self) -> None:
        engine = _engine_basic(adaptive=True)
        assert engine._controller is not None
        before = engine._controller._errors_since_window
        with patch.object(
            engine._graphql_client,
            "create_media",
            side_effect=GraphQLError(
                "reset", operation="CreateMedia", is_retryable=True
            ),
        ):
            with pytest.raises(GraphQLError):
                await engine._create_media_with_retry(
                    "https://x/y.jpg", "obs-1", MediaType.IMAGE, False
                )
        assert engine._controller._errors_since_window > before
