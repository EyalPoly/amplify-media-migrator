from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from googleapiclient.errors import HttpError

from amplify_media_migrator.sources.google_drive import (
    DriveFile,
    FOLDER_MIME_TYPE,
    GoogleDriveClient,
)
from amplify_media_migrator.utils.exceptions import (
    AuthenticationError,
    DownloadError,
    RateLimitError,
)
from amplify_media_migrator.utils.rate_limiter import RateLimiter


@pytest.fixture
def mock_credentials() -> MagicMock:
    return MagicMock()


@pytest.fixture
def client(mock_credentials: MagicMock) -> GoogleDriveClient:
    return GoogleDriveClient(mock_credentials)


@pytest.fixture
def mock_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def connected_client(
    client: GoogleDriveClient, mock_service: MagicMock
) -> GoogleDriveClient:
    client._service = mock_service
    return client


def _make_http_error(status: int, reason: str = "error") -> HttpError:
    resp = MagicMock()
    resp.status = status
    resp.get.return_value = None
    return HttpError(resp=resp, content=reason.encode())


class TestInit:
    def test_default_rate_limiter(self, mock_credentials: MagicMock) -> None:
        client = GoogleDriveClient(mock_credentials)
        assert isinstance(client._rate_limiter, RateLimiter)

    def test_custom_rate_limiter(self, mock_credentials: MagicMock) -> None:
        limiter = RateLimiter(requests_per_second=5.0)
        client = GoogleDriveClient(mock_credentials, rate_limiter=limiter)
        assert client._rate_limiter is limiter

    def test_not_connected_initially(self, client: GoogleDriveClient) -> None:
        assert client._service is None


class TestConnect:
    @patch("amplify_media_migrator.sources.google_drive.build")
    def test_builds_service(
        self, mock_build: MagicMock, client: GoogleDriveClient
    ) -> None:
        mock_service = MagicMock()
        mock_build.return_value = mock_service

        client.connect()

        mock_build.assert_called_once_with(
            "drive", "v3", credentials=client._credentials
        )
        assert client._service is mock_service


class TestEnsureConnected:
    def test_raises_when_not_connected(self, client: GoogleDriveClient) -> None:
        with pytest.raises(DownloadError, match="Not connected"):
            client._ensure_connected()

    def test_returns_service_when_connected(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        assert connected_client._ensure_connected() is mock_service


class TestListFiles:
    def test_single_page(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().list().execute.return_value = {
            "files": [
                {
                    "id": "file1",
                    "name": "photo.jpg",
                    "mimeType": "image/jpeg",
                    "size": "1024",
                    "parents": ["folder1"],
                }
            ],
        }

        files = list(connected_client.list_files("folder1"))

        assert len(files) == 1
        assert files[0] == DriveFile(
            id="file1",
            name="photo.jpg",
            mime_type="image/jpeg",
            size=1024,
            parent_id="folder1",
        )

    def test_multi_page_pagination(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_list = mock_service.files().list
        mock_list.return_value.execute.side_effect = [
            {
                "files": [
                    {
                        "id": "file1",
                        "name": "a.jpg",
                        "mimeType": "image/jpeg",
                        "size": "100",
                        "parents": ["folder1"],
                    }
                ],
                "nextPageToken": "token2",
            },
            {
                "files": [
                    {
                        "id": "file2",
                        "name": "b.jpg",
                        "mimeType": "image/jpeg",
                        "size": "200",
                        "parents": ["folder1"],
                    }
                ],
            },
        ]

        files = list(connected_client.list_files("folder1"))

        assert len(files) == 2
        assert files[0].id == "file1"
        assert files[1].id == "file2"

    def test_recursive_subfolders(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_list = mock_service.files().list
        mock_list.return_value.execute.side_effect = [
            {
                "files": [
                    {
                        "id": "subfolder1",
                        "name": "1-500",
                        "mimeType": FOLDER_MIME_TYPE,
                        "size": "0",
                    },
                    {
                        "id": "file_root",
                        "name": "root.jpg",
                        "mimeType": "image/jpeg",
                        "size": "100",
                        "parents": ["root_folder"],
                    },
                ],
            },
            {
                "files": [
                    {
                        "id": "file_sub",
                        "name": "sub.jpg",
                        "mimeType": "image/jpeg",
                        "size": "200",
                        "parents": ["subfolder1"],
                    }
                ],
            },
        ]

        files = list(connected_client.list_files("root_folder", recursive=True))

        assert len(files) == 2
        names = {f.name for f in files}
        assert names == {"sub.jpg", "root.jpg"}

    def test_non_recursive_skips_subfolders(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().list().execute.return_value = {
            "files": [
                {
                    "id": "subfolder1",
                    "name": "1-500",
                    "mimeType": FOLDER_MIME_TYPE,
                    "size": "0",
                },
                {
                    "id": "file1",
                    "name": "photo.jpg",
                    "mimeType": "image/jpeg",
                    "size": "100",
                    "parents": ["folder1"],
                },
            ],
        }

        files = list(connected_client.list_files("folder1", recursive=False))

        assert len(files) == 1
        assert files[0].name == "photo.jpg"

    def test_empty_folder(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().list().execute.return_value = {"files": []}

        files = list(connected_client.list_files("empty_folder"))

        assert files == []

    def test_file_without_parents(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().list().execute.return_value = {
            "files": [
                {
                    "id": "file1",
                    "name": "orphan.jpg",
                    "mimeType": "image/jpeg",
                    "size": "100",
                }
            ],
        }

        files = list(connected_client.list_files("folder1"))

        assert len(files) == 1
        assert files[0].parent_id is None

    def test_http_error_propagates(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().list().execute.side_effect = _make_http_error(500)

        with pytest.raises(DownloadError):
            list(connected_client.list_files("folder1"))


class TestDownloadFile:
    @patch("amplify_media_migrator.sources.google_drive.MediaIoBaseDownload")
    def test_success(
        self,
        mock_download_cls: MagicMock,
        connected_client: GoogleDriveClient,
        mock_service: MagicMock,
    ) -> None:
        content = b"file content bytes"
        mock_downloader = MagicMock()
        mock_downloader.next_chunk.side_effect = [
            (MagicMock(progress=Mock(return_value=0.5)), False),
            (MagicMock(progress=Mock(return_value=1.0)), True),
        ]
        mock_download_cls.return_value = mock_downloader

        # Simulate writing content to the buffer
        def capture_buffer(buf: MagicMock, req: MagicMock) -> MagicMock:
            buf.write(content)
            return mock_downloader

        mock_download_cls.side_effect = capture_buffer

        result = connected_client.download_file("file1")

        assert result == content
        mock_service.files().get_media.assert_called_once_with(
            fileId="file1", supportsAllDrives=True
        )

    def test_404_raises_download_error(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().get_media.side_effect = _make_http_error(404, "not found")

        with pytest.raises(DownloadError, match="not found"):
            connected_client.download_file("missing_file")

    def test_429_raises_rate_limit_error(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().get_media.side_effect = _make_http_error(429)

        with pytest.raises(RateLimitError, match="rate limit"):
            connected_client.download_file("file1")

    def test_not_connected_raises(self, client: GoogleDriveClient) -> None:
        with pytest.raises(DownloadError, match="Not connected"):
            client.download_file("file1")


class TestDownloadFileToPath:
    @patch.object(GoogleDriveClient, "download_file")
    def test_writes_to_destination(
        self,
        mock_download: MagicMock,
        connected_client: GoogleDriveClient,
        tmp_path: Path,
    ) -> None:
        content = b"photo data"
        mock_download.return_value = content
        dest = tmp_path / "output" / "photo.jpg"

        connected_client.download_file_to_path("file1", dest)

        assert dest.exists()
        assert dest.read_bytes() == content

    @patch.object(GoogleDriveClient, "download_file")
    def test_creates_parent_directories(
        self,
        mock_download: MagicMock,
        connected_client: GoogleDriveClient,
        tmp_path: Path,
    ) -> None:
        mock_download.return_value = b"data"
        dest = tmp_path / "nested" / "dir" / "file.jpg"

        connected_client.download_file_to_path("file1", dest)

        assert dest.parent.exists()
        assert dest.exists()


class TestGetFileMetadata:
    def test_returns_drive_file(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().get().execute.return_value = {
            "id": "file1",
            "name": "photo.jpg",
            "mimeType": "image/jpeg",
            "size": "2048",
            "parents": ["folder1"],
        }

        result = connected_client.get_file_metadata("file1")

        assert result == DriveFile(
            id="file1",
            name="photo.jpg",
            mime_type="image/jpeg",
            size=2048,
            parent_id="folder1",
        )

    def test_file_without_parents(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().get().execute.return_value = {
            "id": "file1",
            "name": "photo.jpg",
            "mimeType": "image/jpeg",
            "size": "100",
        }

        result = connected_client.get_file_metadata("file1")

        assert result.parent_id is None

    def test_404_raises_download_error(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().get().execute.side_effect = _make_http_error(404)

        with pytest.raises(DownloadError):
            connected_client.get_file_metadata("missing")


class TestGetFolderName:
    def test_returns_name(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().get().execute.return_value = {"name": "My Folder"}

        result = connected_client.get_folder_name("folder1")

        assert result == "My Folder"

    def test_404_raises_download_error(
        self, connected_client: GoogleDriveClient, mock_service: MagicMock
    ) -> None:
        mock_service.files().get().execute.side_effect = _make_http_error(404)

        with pytest.raises(DownloadError):
            connected_client.get_folder_name("missing_folder")


class TestErrorHandling:
    def test_401_raises_authentication_error(
        self, connected_client: GoogleDriveClient
    ) -> None:
        with pytest.raises(AuthenticationError, match="401"):
            connected_client._handle_http_error(_make_http_error(401))

    def test_403_raises_authentication_error(
        self, connected_client: GoogleDriveClient
    ) -> None:
        with pytest.raises(AuthenticationError, match="403"):
            connected_client._handle_http_error(_make_http_error(403))

    def test_429_raises_rate_limit_error(
        self, connected_client: GoogleDriveClient
    ) -> None:
        with pytest.raises(RateLimitError, match="rate limit"):
            connected_client._handle_http_error(_make_http_error(429))

    def test_429_with_retry_after_header(
        self, connected_client: GoogleDriveClient
    ) -> None:
        error = _make_http_error(429)
        error.resp.get.return_value = "30"

        with pytest.raises(RateLimitError) as exc_info:
            connected_client._handle_http_error(error)

        assert exc_info.value.retry_after == 30.0

    def test_404_raises_download_error(
        self, connected_client: GoogleDriveClient
    ) -> None:
        with pytest.raises(DownloadError, match="not found"):
            connected_client._handle_http_error(_make_http_error(404), file_id="file1")

    def test_500_raises_download_error(
        self, connected_client: GoogleDriveClient
    ) -> None:
        with pytest.raises(DownloadError, match="500"):
            connected_client._handle_http_error(_make_http_error(500))

    def test_file_id_preserved_in_error(
        self, connected_client: GoogleDriveClient
    ) -> None:
        with pytest.raises(DownloadError) as exc_info:
            connected_client._handle_http_error(
                _make_http_error(500), file_id="my_file"
            )

        assert exc_info.value.file_id == "my_file"

    def test_auth_error_has_provider(self, connected_client: GoogleDriveClient) -> None:
        with pytest.raises(AuthenticationError) as exc_info:
            connected_client._handle_http_error(_make_http_error(401))

        assert exc_info.value.provider == "google_drive"
