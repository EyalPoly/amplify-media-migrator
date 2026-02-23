import asyncio
from typing import Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from amplify_media_migrator.migration.engine import MigrationEngine, SAVE_INTERVAL
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
    return MagicMock(spec=GraphQLClient)


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
        result = asyncio.get_event_loop().run_until_complete(engine.scan("folder-1"))

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
        result = asyncio.get_event_loop().run_until_complete(engine.scan("folder-1"))

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
        result = asyncio.get_event_loop().run_until_complete(engine.scan("folder-1"))

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
        asyncio.get_event_loop().run_until_complete(engine.scan("folder-1"))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

        drive_client.download_file.assert_called_once_with("f1")
        storage_client.upload_file.assert_called_once_with(
            b"photo bytes", "media/obs-1/6602.jpg", "image/jpeg"
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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

        storage_client.upload_file.assert_called_once_with(
            b"photo bytes", "media/obs-1/6602a.jpg", "image/jpeg"
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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

        fp = progress.files["f1"]
        assert fp.status == FileStatus.PARTIAL
        assert fp.media_ids == ["m-a"]

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(
            engine.process_file(file, dry_run=True)
        )

        drive_client.download_file.assert_not_called()
        storage_client.upload_file.assert_not_called()
        graphql_client.create_media.assert_not_called()

        fp = progress.files["f1"]
        assert fp.status == FileStatus.COMPLETED
        assert fp.observation_ids == ["obs-1"]


class TestSkipExisting:
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

        asyncio.get_event_loop().run_until_complete(
            engine.process_file(file, skip_existing=True)
        )

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

        asyncio.get_event_loop().run_until_complete(
            engine.process_file(file, skip_existing=True)
        )

        drive_client.download_file.assert_called_once()
        assert progress.files["f1"].status == FileStatus.COMPLETED


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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        graphql_client.get_observations_by_sequential_ids.side_effect = GraphQLError(
            "Server error", operation="query"
        )

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        graphql_client.get_observations_by_sequential_ids.side_effect = (
            AuthenticationError("Token expired", provider="cognito")
        )

        with pytest.raises(AuthenticationError):
            asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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
            asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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
            asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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
            asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

        assert drive_client.download_file.call_count == 2
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
        graphql_client.get_observations_by_sequential_ids.side_effect = [
            {6602: obs1},
            {6603: obs2},
        ]
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.side_effect = [
            "https://bucket/media/obs-1/6602.jpg",
            "https://bucket/media/obs-2/6603.jpg",
        ]
        graphql_client.create_media.side_effect = [
            _media("m-1", obs_id="obs-1"),
            _media("m-2", obs_id="obs-2"),
        ]

        asyncio.get_event_loop().run_until_complete(engine.migrate("folder-1"))

        assert progress.files["f1"].status == FileStatus.COMPLETED
        assert progress.files["f2"].status == FileStatus.COMPLETED

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

        asyncio.get_event_loop().run_until_complete(engine.migrate("folder-1"))

        assert drive_client.download_file.call_count == 1
        assert progress.files["f2"].status == FileStatus.COMPLETED


class TestResume:
    def test_raises_when_no_progress_file(
        self,
        engine: MigrationEngine,
    ) -> None:
        with pytest.raises(MigratorError, match="No progress file"):
            asyncio.get_event_loop().run_until_complete(
                engine.resume("nonexistent-folder")
            )

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
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.get_event_loop().run_until_complete(engine.resume("folder-1"))

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
        storage_client.upload_file.return_value = (
            "https://bucket/media/obs-a/6000-6001.jpg"
        )
        graphql_client.create_media.side_effect = [
            _media("m-a", obs_id="obs-a"),
            _media("m-b", obs_id="obs-b"),
        ]

        asyncio.get_event_loop().run_until_complete(engine.resume("folder-1"))

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

        asyncio.get_event_loop().run_until_complete(engine.resume("folder-1"))


class TestProgressCallback:
    def test_callback_called_on_completion(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        storage_client: MagicMock,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        callback = MagicMock()
        engine.set_progress_callback(callback)

        file = _drive_file("f1", "6602.jpg")
        obs = _observation("obs-1", 6602)
        graphql_client.get_observations_by_sequential_ids.return_value = {6602: obs}
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.return_value = "https://bucket/media/obs-1/6602.jpg"
        graphql_client.create_media.return_value = _media("m-1")

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

        callback.assert_called_once_with("6602.jpg", FileStatus.COMPLETED)

    def test_callback_called_on_orphan(
        self,
        engine: MigrationEngine,
        graphql_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        progress.load("folder-1")
        callback = MagicMock()
        engine.set_progress_callback(callback)

        file = _drive_file("f1", "99999.jpg")
        graphql_client.get_observations_by_sequential_ids.return_value = {}

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

        callback.assert_called_once_with("99999.jpg", FileStatus.ORPHAN)


class TestEdgeCases:
    def test_empty_folder_scan(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        drive_client.list_files.return_value = []
        result = asyncio.get_event_loop().run_until_complete(engine.scan("folder-1"))

        assert sum(result.values()) == 0
        assert progress.total_files == 0

    def test_empty_folder_migrate(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
    ) -> None:
        drive_client.list_files.return_value = []
        asyncio.get_event_loop().run_until_complete(engine.migrate("folder-1"))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

        graphql_client.create_media.assert_called_once_with(
            "https://bucket/media/obs-1/6602.jpg",
            "obs-1",
            MediaType.IMAGE,
            True,
        )

    def test_skip_existing_query_error_falls_through(
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

        asyncio.get_event_loop().run_until_complete(
            engine.process_file(file, skip_existing=True)
        )

        drive_client.download_file.assert_called_once()
        assert progress.files["f1"].status == FileStatus.COMPLETED

    def test_skip_existing_auth_error_propagates(
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
            asyncio.get_event_loop().run_until_complete(
                engine.process_file(file, skip_existing=True)
            )

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        def mock_get_obs(seq_ids: list) -> dict:
            return {sid: _observation(f"obs-{sid}", sid) for sid in seq_ids}

        graphql_client.get_observations_by_sequential_ids.side_effect = mock_get_obs
        drive_client.download_file.return_value = b"data"
        storage_client.upload_file.side_effect = [
            f"https://bucket/media/obs-{6600 + i}/{6600 + i}.jpg" for i in range(5)
        ]
        graphql_client.create_media.side_effect = [_media(f"m-{i}") for i in range(5)]

        asyncio.get_event_loop().run_until_complete(engine.migrate("folder-1"))

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

        asyncio.get_event_loop().run_until_complete(engine.migrate("folder-1"))

        assert progress.files["f1"].status == FileStatus.COMPLETED
        assert progress.files["f2"].status == FileStatus.NEEDS_REVIEW
        assert drive_client.download_file.call_count == 1

    def test_resume_metadata_fetch_failure(
        self,
        engine: MigrationEngine,
        drive_client: MagicMock,
        progress: ProgressTracker,
    ) -> None:
        """When metadata fetch fails during resume, file is marked failed."""
        progress.load("folder-1")
        progress.update_file(
            file_id="f1",
            filename="6602.jpg",
            status=FileStatus.PENDING,
        )
        progress.save()

        drive_client.get_file_metadata.side_effect = DownloadError(
            "File not found", file_id="f1"
        )

        asyncio.get_event_loop().run_until_complete(engine.resume("folder-1"))

        assert progress.files["f1"].status == FileStatus.FAILED
        assert "Could not fetch file metadata" in (progress.files["f1"].error or "")

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

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

        asyncio.get_event_loop().run_until_complete(engine.process_file(file))

        fp = progress.files["f1"]
        assert fp.status == FileStatus.COMPLETED
        assert fp.error is None


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
