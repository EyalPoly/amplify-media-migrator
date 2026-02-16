from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from amplify_media_migrator.auth.google_drive import GoogleDriveAuthProvider


@pytest.fixture
def credentials_path(tmp_path: Path) -> Path:
    return tmp_path / "credentials.json"


@pytest.fixture
def token_path(tmp_path: Path) -> Path:
    return tmp_path / "tokens" / "token.json"


@pytest.fixture
def provider(credentials_path: Path, token_path: Path) -> GoogleDriveAuthProvider:
    return GoogleDriveAuthProvider(credentials_path, token_path)


class TestInit:
    def test_initialization(self, provider: GoogleDriveAuthProvider) -> None:
        assert provider.get_credentials() is None
        assert provider.is_authenticated() is False


class TestIsAuthenticated:
    def test_no_credentials(self, provider: GoogleDriveAuthProvider) -> None:
        assert provider.is_authenticated() is False

    def test_valid_credentials(self, provider: GoogleDriveAuthProvider) -> None:
        mock_creds = MagicMock()
        mock_creds.valid = True
        provider._credentials = mock_creds
        assert provider.is_authenticated() is True

    def test_invalid_credentials(self, provider: GoogleDriveAuthProvider) -> None:
        mock_creds = MagicMock()
        mock_creds.valid = False
        provider._credentials = mock_creds
        assert provider.is_authenticated() is False


class TestGetCredentials:
    def test_returns_none_initially(self, provider: GoogleDriveAuthProvider) -> None:
        assert provider.get_credentials() is None

    def test_returns_credentials_when_set(
        self, provider: GoogleDriveAuthProvider
    ) -> None:
        mock_creds = MagicMock()
        provider._credentials = mock_creds
        assert provider.get_credentials() is mock_creds


class TestLoadToken:
    def test_no_file(self, provider: GoogleDriveAuthProvider) -> None:
        assert provider.load_token() is False
        assert provider._credentials is None

    @patch("amplify_media_migrator.auth.google_drive.Credentials")
    def test_valid_token_file(
        self,
        mock_credentials_cls: MagicMock,
        provider: GoogleDriveAuthProvider,
        token_path: Path,
    ) -> None:
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text('{"token": "test"}')

        mock_creds = MagicMock()
        mock_credentials_cls.from_authorized_user_file.return_value = mock_creds

        assert provider.load_token() is True
        assert provider._credentials is mock_creds
        mock_credentials_cls.from_authorized_user_file.assert_called_once()

    def test_corrupted_token_file(
        self,
        provider: GoogleDriveAuthProvider,
        token_path: Path,
    ) -> None:
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text("not valid json {{{")

        with patch("amplify_media_migrator.auth.google_drive.Credentials") as mock_cls:
            mock_cls.from_authorized_user_file.side_effect = ValueError("Invalid token")
            assert provider.load_token() is False
            assert provider._credentials is None


class TestSaveToken:
    def test_no_credentials(
        self, provider: GoogleDriveAuthProvider, token_path: Path
    ) -> None:
        provider.save_token()
        assert not token_path.exists()

    def test_saves_to_disk(
        self, provider: GoogleDriveAuthProvider, token_path: Path
    ) -> None:
        mock_creds = MagicMock()
        mock_creds.to_json.return_value = '{"token": "saved"}'
        provider._credentials = mock_creds

        provider.save_token()

        assert token_path.exists()
        assert token_path.read_text() == '{"token": "saved"}'

    def test_creates_parent_directories(
        self, provider: GoogleDriveAuthProvider, token_path: Path
    ) -> None:
        assert not token_path.parent.exists()

        mock_creds = MagicMock()
        mock_creds.to_json.return_value = "{}"
        provider._credentials = mock_creds

        provider.save_token()

        assert token_path.parent.exists()
        assert token_path.exists()


class TestRefreshIfNeeded:
    def test_no_credentials(self, provider: GoogleDriveAuthProvider) -> None:
        assert provider.refresh_if_needed() is False

    def test_valid_token(self, provider: GoogleDriveAuthProvider) -> None:
        mock_creds = MagicMock()
        mock_creds.valid = True
        provider._credentials = mock_creds
        assert provider.refresh_if_needed() is True

    @patch("amplify_media_migrator.auth.google_drive.Request")
    def test_refreshes_expired_token(
        self, mock_request_cls: MagicMock, provider: GoogleDriveAuthProvider
    ) -> None:
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = "refresh_token_value"
        mock_creds.to_json.return_value = "{}"
        provider._credentials = mock_creds

        assert provider.refresh_if_needed() is True
        mock_creds.refresh.assert_called_once()

    def test_no_refresh_token(self, provider: GoogleDriveAuthProvider) -> None:
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = None
        provider._credentials = mock_creds
        assert provider.refresh_if_needed() is False

    def test_not_expired(self, provider: GoogleDriveAuthProvider) -> None:
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = False
        provider._credentials = mock_creds
        assert provider.refresh_if_needed() is False

    @patch("amplify_media_migrator.auth.google_drive.Request")
    def test_refresh_failure(
        self, mock_request_cls: MagicMock, provider: GoogleDriveAuthProvider
    ) -> None:
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = "refresh_token_value"
        mock_creds.refresh.side_effect = Exception("Network error")
        provider._credentials = mock_creds

        assert provider.refresh_if_needed() is False


class TestAuthenticate:
    def test_uses_existing_valid_token(
        self,
        provider: GoogleDriveAuthProvider,
        token_path: Path,
    ) -> None:
        with patch.object(provider, "load_token") as mock_load:
            mock_load.return_value = True
            mock_creds = MagicMock()
            mock_creds.valid = True
            provider._credentials = mock_creds

            assert provider.authenticate() is True

    def test_refreshes_expired_token(
        self,
        provider: GoogleDriveAuthProvider,
    ) -> None:
        with (
            patch.object(provider, "load_token") as mock_load,
            patch.object(provider, "refresh_if_needed") as mock_refresh,
        ):
            mock_load.return_value = True
            mock_creds = MagicMock()
            mock_creds.valid = False
            provider._credentials = mock_creds
            mock_refresh.return_value = True

            assert provider.authenticate() is True
            mock_refresh.assert_called_once()

    def test_runs_oauth_flow_when_no_token(
        self,
        provider: GoogleDriveAuthProvider,
    ) -> None:
        with (
            patch.object(provider, "load_token", return_value=False),
            patch.object(provider, "_run_oauth_flow", return_value=True) as mock_flow,
        ):
            assert provider.authenticate() is True
            mock_flow.assert_called_once()

    def test_handles_exception(
        self,
        provider: GoogleDriveAuthProvider,
    ) -> None:
        with patch.object(provider, "load_token", side_effect=Exception("Unexpected")):
            assert provider.authenticate() is False


class TestRunOAuthFlow:
    @patch("amplify_media_migrator.auth.google_drive.InstalledAppFlow")
    def test_missing_credentials_file(
        self,
        mock_flow_cls: MagicMock,
        provider: GoogleDriveAuthProvider,
    ) -> None:
        assert provider._run_oauth_flow() is False
        mock_flow_cls.from_client_secrets_file.assert_not_called()

    @patch("amplify_media_migrator.auth.google_drive.InstalledAppFlow")
    def test_runs_browser_flow(
        self,
        mock_flow_cls: MagicMock,
        provider: GoogleDriveAuthProvider,
        credentials_path: Path,
    ) -> None:
        credentials_path.write_text('{"installed": {}}')

        mock_flow = MagicMock()
        mock_creds = MagicMock()
        mock_creds.to_json.return_value = "{}"
        mock_flow.run_local_server.return_value = mock_creds
        mock_flow_cls.from_client_secrets_file.return_value = mock_flow

        assert provider._run_oauth_flow() is True
        assert provider._credentials is mock_creds
        mock_flow.run_local_server.assert_called_once_with(port=0)

    @patch("amplify_media_migrator.auth.google_drive.InstalledAppFlow")
    def test_oauth_flow_failure(
        self,
        mock_flow_cls: MagicMock,
        provider: GoogleDriveAuthProvider,
        credentials_path: Path,
    ) -> None:
        credentials_path.write_text('{"installed": {}}')
        mock_flow_cls.from_client_secrets_file.side_effect = Exception("OAuth error")

        assert provider._run_oauth_flow() is False
