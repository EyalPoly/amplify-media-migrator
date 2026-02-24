import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from amplify_media_migrator.cli import (
    _authenticate_cognito,
    _authenticate_google,
    _create_engine,
    _load_config,
    _print_summary,
    _run_with_progress,
    export,
    main,
    migrate,
    resume,
    review,
    scan,
    show,
)
from amplify_media_migrator.config import (
    Config,
    ConfigManager,
    ConfigurationError,
    MigrationConfig,
)
from amplify_media_migrator.migration.engine import MigrationEngine
from amplify_media_migrator.migration.progress import FileStatus, ProgressTracker

pytestmark = pytest.mark.unit


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def sample_config_dict() -> dict:
    return {
        "google_drive": {
            "folder_id": "test-folder",
            "credentials_path": "/tmp/creds.json",
            "token_path": "/tmp/token.json",
        },
        "aws": {
            "region": "us-east-1",
            "cognito": {
                "user_pool_id": "us-east-1_Test",
                "client_id": "test-client",
                "identity_pool_id": "us-east-1:test-pool",
                "username": "test@example.com",
            },
            "amplify": {
                "api_endpoint": "https://test.appsync-api.us-east-1.amazonaws.com/graphql",
                "storage_bucket": "test-bucket",
            },
        },
        "migration": {
            "concurrency": 10,
            "retry_attempts": 3,
            "retry_delay_seconds": 5,
            "chunk_size_mb": 8,
            "default_media_public": False,
        },
    }


@pytest.fixture
def config_file(tmp_path: Path, sample_config_dict: dict) -> Path:
    path = tmp_path / "config.json"
    path.write_text(json.dumps(sample_config_dict))
    return path


class TestMainGroup:
    def test_main_help(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "Usage" in result.output

    def test_main_version(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0


class TestConfigCommand:
    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_new_config(self, mock_mgr_cls: MagicMock, runner: CliRunner) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = False
        mock_mgr.config = Config()
        mock_mgr.config_path = Path("/tmp/config.json")

        result = runner.invoke(main, ["config"], input="\n" * 10)
        assert result.exit_code == 0
        assert "Creating a new one" in result.output
        mock_mgr.save.assert_called_once()

    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_existing_config_abort(
        self, mock_mgr_cls: MagicMock, runner: CliRunner
    ) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = True
        mock_mgr.config_path = Path("/tmp/config.json")

        result = runner.invoke(main, ["config"], input="n\n")
        assert result.exit_code == 0
        assert "Aborted" in result.output
        mock_mgr.save.assert_not_called()

    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_existing_config_overwrite(
        self, mock_mgr_cls: MagicMock, runner: CliRunner
    ) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = True
        mock_mgr.config = Config()
        mock_mgr.config_path = Path("/tmp/config.json")

        result = runner.invoke(main, ["config"], input="y\n" + "\n" * 10)
        assert result.exit_code == 0
        mock_mgr.load.assert_called_once()
        mock_mgr.save.assert_called_once()

    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_validation_error_exits(
        self, mock_mgr_cls: MagicMock, runner: CliRunner
    ) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = False
        mock_config = MagicMock()
        mock_config.validate.side_effect = ConfigurationError("bad config")
        mock_mgr.config = mock_config

        result = runner.invoke(main, ["config"], input="\n" * 10)
        assert result.exit_code == 1
        assert "Validation error" in result.output


class TestShowCommand:
    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_show_no_config(self, mock_mgr_cls: MagicMock, runner: CliRunner) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = False
        mock_mgr.config_path = Path("/tmp/config.json")

        result = runner.invoke(main, ["show"])
        assert result.exit_code == 1
        assert "No configuration file found" in result.output

    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_show_displays_config(
        self, mock_mgr_cls: MagicMock, runner: CliRunner
    ) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = True
        mock_mgr.config_path = Path("/tmp/config.json")
        config = Config()
        mock_mgr.load.return_value = config

        result = runner.invoke(main, ["show"])
        assert result.exit_code == 0
        assert "google_drive" in result.output
        assert "aws" in result.output

    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_show_load_error(self, mock_mgr_cls: MagicMock, runner: CliRunner) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = True
        mock_mgr.load.side_effect = ConfigurationError("corrupt")

        result = runner.invoke(main, ["show"])
        assert result.exit_code == 1
        assert "Error loading configuration" in result.output


class TestLoadConfig:
    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_no_config_exits(self, mock_mgr_cls: MagicMock) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = False

        with pytest.raises(SystemExit):
            _load_config()

    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_config_error_exits(self, mock_mgr_cls: MagicMock) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = True
        mock_mgr.load.side_effect = ConfigurationError("bad")

        with pytest.raises(SystemExit):
            _load_config()

    @patch("amplify_media_migrator.cli.ConfigManager")
    def test_success(self, mock_mgr_cls: MagicMock) -> None:
        mock_mgr = MagicMock()
        mock_mgr_cls.return_value = mock_mgr
        mock_mgr.exists.return_value = True

        result = _load_config()
        assert result is mock_mgr
        mock_mgr.load.assert_called_once()


class TestAuthenticateGoogle:
    def test_success(self) -> None:
        mock_cfg = MagicMock()
        mock_cfg.get.side_effect = lambda key: {
            "google_drive.credentials_path": "/tmp/creds.json",
            "google_drive.token_path": "/tmp/token.json",
        }[key]

        with patch(
            "amplify_media_migrator.cli.GoogleDriveAuthProvider"
        ) as mock_auth_cls, patch(
            "amplify_media_migrator.cli.GoogleDriveClient"
        ) as mock_client_cls:
            mock_auth = MagicMock()
            mock_auth.authenticate.return_value = True
            mock_auth.get_credentials.return_value = MagicMock()
            mock_auth_cls.return_value = mock_auth

            mock_client = MagicMock()
            mock_client_cls.return_value = mock_client

            result = _authenticate_google(mock_cfg)
            assert result is mock_client
            mock_client.connect.assert_called_once()

    def test_auth_failure_exits(self) -> None:
        mock_cfg = MagicMock()
        mock_cfg.get.side_effect = lambda key: "/tmp/path.json"

        with patch(
            "amplify_media_migrator.cli.GoogleDriveAuthProvider"
        ) as mock_auth_cls:
            mock_auth = MagicMock()
            mock_auth.authenticate.return_value = False
            mock_auth_cls.return_value = mock_auth

            with pytest.raises(SystemExit):
                _authenticate_google(mock_cfg)


class TestAuthenticateCognito:
    @patch("amplify_media_migrator.cli.click.prompt", return_value="password123")
    @patch("amplify_auth.CognitoAuthProvider")
    def test_success(self, mock_cognito_cls: MagicMock, mock_prompt: MagicMock) -> None:
        mock_cfg = MagicMock()
        mock_cfg.get.side_effect = lambda key: {
            "aws.cognito.user_pool_id": "pool",
            "aws.cognito.client_id": "client",
            "aws.region": "us-east-1",
            "aws.cognito.username": "user@test.com",
        }[key]

        mock_cognito = MagicMock()
        mock_cognito.authenticate.return_value = True
        mock_cognito.get_id_token.return_value = "test-token"
        mock_cognito_cls.return_value = mock_cognito

        result = _authenticate_cognito(mock_cfg)
        assert result == "test-token"

    @patch("amplify_media_migrator.cli.click.prompt", return_value="password")
    @patch("amplify_auth.CognitoAuthProvider")
    def test_auth_failure_exits(
        self, mock_cognito_cls: MagicMock, mock_prompt: MagicMock
    ) -> None:
        mock_cfg = MagicMock()
        mock_cfg.get.side_effect = lambda key: "value"

        mock_cognito = MagicMock()
        mock_cognito.authenticate.return_value = False
        mock_cognito_cls.return_value = mock_cognito

        with pytest.raises(SystemExit):
            _authenticate_cognito(mock_cfg)

    @patch("amplify_media_migrator.cli.click.prompt", return_value="password")
    @patch("amplify_auth.CognitoAuthProvider")
    def test_no_token_exits(
        self, mock_cognito_cls: MagicMock, mock_prompt: MagicMock
    ) -> None:
        mock_cfg = MagicMock()
        mock_cfg.get.side_effect = lambda key: "value"

        mock_cognito = MagicMock()
        mock_cognito.authenticate.return_value = True
        mock_cognito.get_id_token.return_value = None
        mock_cognito_cls.return_value = mock_cognito

        with pytest.raises(SystemExit):
            _authenticate_cognito(mock_cfg)


class TestCreateEngine:
    def test_creates_engine_with_config(self) -> None:
        mock_cfg = MagicMock()
        mock_cfg.get.side_effect = lambda key: {
            "aws.amplify.storage_bucket": "test-bucket",
            "aws.region": "us-east-1",
            "aws.cognito.identity_pool_id": "pool-id",
            "aws.cognito.user_pool_id": "user-pool",
            "aws.amplify.api_endpoint": "https://test.api.com/graphql",
        }[key]
        mock_cfg.config = Config(migration=MigrationConfig(concurrency=5))

        with patch(
            "amplify_media_migrator.cli.AmplifyStorageClient"
        ) as mock_storage_cls, patch(
            "amplify_media_migrator.cli.GraphQLClient"
        ) as mock_gql_cls:
            mock_storage = MagicMock()
            mock_storage_cls.return_value = mock_storage
            mock_gql = MagicMock()
            mock_gql_cls.return_value = mock_gql
            mock_drive = MagicMock()

            engine = _create_engine(mock_cfg, mock_drive, "token", concurrency=5)
            assert isinstance(engine, MigrationEngine)
            mock_storage.connect.assert_called_once_with("token")
            mock_gql.connect.assert_called_once_with("token")


class TestRunWithProgress:
    def test_runs_coroutine_without_tqdm(self) -> None:
        called = False

        async def coro() -> None:
            nonlocal called
            called = True

        mock_engine = MagicMock(spec=MigrationEngine)

        import builtins

        real_import = builtins.__import__

        def mock_import(name: str, *args: object, **kwargs: object) -> object:
            if name == "tqdm":
                raise ImportError("no tqdm")
            return real_import(name, *args, **kwargs)

        with patch.object(builtins, "__import__", side_effect=mock_import):
            _run_with_progress(coro, mock_engine)

        assert called

    def test_runs_with_tqdm(self) -> None:
        called = False

        async def coro() -> None:
            nonlocal called
            called = True

        mock_engine = MagicMock(spec=MigrationEngine)

        _run_with_progress(coro, mock_engine, desc="Testing")

        assert called
        mock_engine.set_progress_callback.assert_called_once()

    def test_tqdm_progress_callback_updates_bar(self) -> None:
        captured_callback = None

        async def coro() -> None:
            pass

        mock_engine = MagicMock(spec=MigrationEngine)

        def capture_callback(cb: object) -> None:
            nonlocal captured_callback
            captured_callback = cb

        mock_engine.set_progress_callback.side_effect = capture_callback

        _run_with_progress(coro, mock_engine, desc="Test")

        assert captured_callback is not None
        captured_callback("6602.jpg", FileStatus.COMPLETED)


class TestPrintSummary:
    def test_prints_all_fields(self, runner: CliRunner) -> None:
        summary = {
            "total": 100,
            "completed": 80,
            "failed": 5,
            "orphan": 10,
            "needs_review": 3,
            "partial": 1,
            "pending": 1,
        }
        result = runner.invoke(main, ["--help"])
        _print_summary(summary)


class TestScanCommand:
    @patch("amplify_media_migrator.cli._load_config")
    @patch("amplify_media_migrator.cli._authenticate_google")
    @patch("amplify_media_migrator.cli.asyncio.run")
    @patch("amplify_media_migrator.cli.AmplifyStorageClient")
    @patch("amplify_media_migrator.cli.GraphQLClient")
    def test_scan_success(
        self,
        mock_gql_cls: MagicMock,
        mock_storage_cls: MagicMock,
        mock_run: MagicMock,
        mock_auth: MagicMock,
        mock_load: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_cfg = MagicMock()
        mock_cfg.get.side_effect = lambda key: "test-value"
        mock_load.return_value = mock_cfg
        mock_auth.return_value = MagicMock()
        mock_run.return_value = {
            "single": 10,
            "multiple": 3,
            "range": 1,
            "invalid": 2,
        }

        with patch.object(MigrationEngine, "get_summary") as mock_summary:
            mock_summary.return_value = {
                "total": 16,
                "completed": 0,
                "failed": 0,
                "orphan": 0,
                "needs_review": 2,
                "partial": 0,
                "pending": 14,
            }
            result = runner.invoke(main, ["scan", "--folder-id", "test-folder"])

        assert result.exit_code == 0
        assert "Scanning folder test-folder" in result.output
        assert "16 files found" in result.output
        assert "Single:" in result.output

    def test_scan_missing_folder_id(self, runner: CliRunner) -> None:
        result = runner.invoke(main, ["scan"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "required" in result.output.lower()


class TestReviewCommand:
    def test_review_no_progress(self, runner: CliRunner, tmp_path: Path) -> None:
        with patch("amplify_media_migrator.cli.ProgressTracker") as mock_tracker_cls:
            mock_tracker = MagicMock()
            mock_tracker.load.return_value = False
            mock_tracker_cls.return_value = mock_tracker

            result = runner.invoke(main, ["review", "--folder-id", "test"])
            assert result.exit_code == 1
            assert "No progress file" in result.output

    def test_review_no_files(self, runner: CliRunner) -> None:
        with patch("amplify_media_migrator.cli.ProgressTracker") as mock_tracker_cls:
            mock_tracker = MagicMock()
            mock_tracker.load.return_value = True
            mock_tracker.get_files_by_status.return_value = []
            mock_tracker_cls.return_value = mock_tracker

            result = runner.invoke(main, ["review", "--folder-id", "test"])
            assert result.exit_code == 0
            assert "No files need review" in result.output

    def test_review_shows_files(self, runner: CliRunner) -> None:
        from amplify_media_migrator.migration.progress import FileProgress

        fp1 = FileProgress(
            filename="bad.txt",
            status=FileStatus.NEEDS_REVIEW,
            error="Unsupported extension",
        )
        fp2 = FileProgress(
            filename="noext",
            status=FileStatus.NEEDS_REVIEW,
            error="Missing file extension",
        )

        with patch("amplify_media_migrator.cli.ProgressTracker") as mock_tracker_cls:
            mock_tracker = MagicMock()
            mock_tracker.load.return_value = True
            mock_tracker.get_files_by_status.return_value = [fp1, fp2]
            mock_tracker_cls.return_value = mock_tracker

            result = runner.invoke(main, ["review", "--folder-id", "test"])
            assert result.exit_code == 0
            assert "Files needing review: 2" in result.output
            assert "bad.txt" in result.output
            assert "Unsupported extension" in result.output
            assert "noext" in result.output


class TestExportCommand:
    def test_export_no_progress(self, runner: CliRunner) -> None:
        with patch("amplify_media_migrator.cli.ProgressTracker") as mock_tracker_cls:
            mock_tracker = MagicMock()
            mock_tracker.load.return_value = False
            mock_tracker_cls.return_value = mock_tracker

            result = runner.invoke(
                main,
                [
                    "export",
                    "--folder-id",
                    "test",
                    "--status",
                    "orphan",
                    "--output",
                    "/tmp/out.json",
                ],
            )
            assert result.exit_code == 1
            assert "No progress file" in result.output

    def test_export_success(self, runner: CliRunner, tmp_path: Path) -> None:
        output_file = tmp_path / "export.json"

        with patch("amplify_media_migrator.cli.ProgressTracker") as mock_tracker_cls:
            mock_tracker = MagicMock()
            mock_tracker.load.return_value = True
            mock_tracker.export_to_json.return_value = 5
            mock_tracker_cls.return_value = mock_tracker

            result = runner.invoke(
                main,
                [
                    "export",
                    "--folder-id",
                    "test",
                    "--status",
                    "failed",
                    "--output",
                    str(output_file),
                ],
            )
            assert result.exit_code == 0
            assert "Exported 5 files" in result.output

    def test_export_invalid_status(self, runner: CliRunner) -> None:
        result = runner.invoke(
            main,
            [
                "export",
                "--folder-id",
                "test",
                "--status",
                "invalid_status",
                "--output",
                "/tmp/x",
            ],
        )
        assert result.exit_code != 0


class TestMigrateCommand:
    @patch("amplify_media_migrator.cli._run_with_progress")
    @patch("amplify_media_migrator.cli._create_engine")
    @patch("amplify_media_migrator.cli._authenticate_cognito")
    @patch("amplify_media_migrator.cli._authenticate_google")
    @patch("amplify_media_migrator.cli._load_config")
    def test_migrate_success(
        self,
        mock_load: MagicMock,
        mock_auth_g: MagicMock,
        mock_auth_c: MagicMock,
        mock_create: MagicMock,
        mock_run_progress: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_engine = MagicMock()
        mock_engine.get_summary.return_value = {
            "total": 10,
            "completed": 10,
            "failed": 0,
            "orphan": 0,
            "needs_review": 0,
            "partial": 0,
            "pending": 0,
        }
        mock_create.return_value = mock_engine

        result = runner.invoke(main, ["migrate", "--folder-id", "test"])
        assert result.exit_code == 0
        assert "Starting migration" in result.output
        assert "Migration Summary" in result.output

    @patch("amplify_media_migrator.cli._run_with_progress")
    @patch("amplify_media_migrator.cli._create_engine")
    @patch("amplify_media_migrator.cli._authenticate_cognito")
    @patch("amplify_media_migrator.cli._authenticate_google")
    @patch("amplify_media_migrator.cli._load_config")
    def test_migrate_dry_run(
        self,
        mock_load: MagicMock,
        mock_auth_g: MagicMock,
        mock_auth_c: MagicMock,
        mock_create: MagicMock,
        mock_run_progress: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_engine = MagicMock()
        mock_engine.get_summary.return_value = {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "orphan": 0,
            "needs_review": 0,
            "partial": 0,
            "pending": 0,
        }
        mock_create.return_value = mock_engine

        result = runner.invoke(main, ["migrate", "--folder-id", "test", "--dry-run"])
        assert result.exit_code == 0
        assert "[DRY RUN]" in result.output

    @patch("amplify_media_migrator.cli._run_with_progress")
    @patch("amplify_media_migrator.cli._create_engine")
    @patch("amplify_media_migrator.cli._authenticate_cognito")
    @patch("amplify_media_migrator.cli._authenticate_google")
    @patch("amplify_media_migrator.cli._load_config")
    def test_migrate_with_concurrency(
        self,
        mock_load: MagicMock,
        mock_auth_g: MagicMock,
        mock_auth_c: MagicMock,
        mock_create: MagicMock,
        mock_run_progress: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_engine = MagicMock()
        mock_engine.get_summary.return_value = {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "orphan": 0,
            "needs_review": 0,
            "partial": 0,
            "pending": 0,
        }
        mock_create.return_value = mock_engine

        result = runner.invoke(
            main, ["migrate", "--folder-id", "test", "--concurrency", "20"]
        )
        assert result.exit_code == 0
        mock_create.assert_called_once()
        assert (
            mock_create.call_args[1].get("concurrency", mock_create.call_args[0][-1])
            == 20
            or 20 in mock_create.call_args[0]
        )


class TestResumeCommand:
    @patch("amplify_media_migrator.cli._run_with_progress")
    @patch("amplify_media_migrator.cli._create_engine")
    @patch("amplify_media_migrator.cli._authenticate_cognito")
    @patch("amplify_media_migrator.cli._authenticate_google")
    @patch("amplify_media_migrator.cli._load_config")
    def test_resume_success(
        self,
        mock_load: MagicMock,
        mock_auth_g: MagicMock,
        mock_auth_c: MagicMock,
        mock_create: MagicMock,
        mock_run_progress: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_engine = MagicMock()
        mock_engine.get_summary.return_value = {
            "total": 10,
            "completed": 10,
            "failed": 0,
            "orphan": 0,
            "needs_review": 0,
            "partial": 0,
            "pending": 0,
        }
        mock_create.return_value = mock_engine

        result = runner.invoke(main, ["resume", "--folder-id", "test"])
        assert result.exit_code == 0
        assert "Resuming migration" in result.output
        assert "Migration Summary" in result.output

    @patch("amplify_media_migrator.cli._run_with_progress")
    @patch("amplify_media_migrator.cli._create_engine")
    @patch("amplify_media_migrator.cli._authenticate_cognito")
    @patch("amplify_media_migrator.cli._authenticate_google")
    @patch("amplify_media_migrator.cli._load_config")
    @patch("amplify_media_migrator.cli.setup_logging")
    def test_resume_verbose(
        self,
        mock_setup_logging: MagicMock,
        mock_load: MagicMock,
        mock_auth_g: MagicMock,
        mock_auth_c: MagicMock,
        mock_create: MagicMock,
        mock_run_progress: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_engine = MagicMock()
        mock_engine.get_summary.return_value = {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "orphan": 0,
            "needs_review": 0,
            "partial": 0,
            "pending": 0,
        }
        mock_create.return_value = mock_engine

        result = runner.invoke(main, ["resume", "--folder-id", "test", "--verbose"])
        assert result.exit_code == 0
        mock_setup_logging.assert_called_once_with(level="DEBUG")


class TestMigrateVerbose:
    @patch("amplify_media_migrator.cli._run_with_progress")
    @patch("amplify_media_migrator.cli._create_engine")
    @patch("amplify_media_migrator.cli._authenticate_cognito")
    @patch("amplify_media_migrator.cli._authenticate_google")
    @patch("amplify_media_migrator.cli._load_config")
    @patch("amplify_media_migrator.cli.setup_logging")
    def test_migrate_verbose(
        self,
        mock_setup_logging: MagicMock,
        mock_load: MagicMock,
        mock_auth_g: MagicMock,
        mock_auth_c: MagicMock,
        mock_create: MagicMock,
        mock_run_progress: MagicMock,
        runner: CliRunner,
    ) -> None:
        mock_engine = MagicMock()
        mock_engine.get_summary.return_value = {
            "total": 0,
            "completed": 0,
            "failed": 0,
            "orphan": 0,
            "needs_review": 0,
            "partial": 0,
            "pending": 0,
        }
        mock_create.return_value = mock_engine

        result = runner.invoke(main, ["migrate", "--folder-id", "test", "--verbose"])
        assert result.exit_code == 0
        mock_setup_logging.assert_called_once_with(level="DEBUG")
