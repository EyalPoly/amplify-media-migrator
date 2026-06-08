import asyncio
import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable, List, Optional

import click

from .auth.google_drive import GoogleDriveAuthProvider
from .auth.token_manager import CognitoTokenManager
from .cli_progress import LiveReporter
from .config import ConfigManager, ConfigurationError, config_to_dict
from .migration.engine import MigrationEngine
from .migration.mapper import FilenameMapper
from .migration.progress import FileStatus, ProgressTracker
from .utils.logger import DEFAULT_LOG_FORMAT
from .sources.google_drive import GoogleDriveClient
from .targets.amplify_storage import AmplifyStorageClient
from .targets.graphql_client import GraphQLClient
from .utils.exceptions import AuthenticationError, MigratorError
from .utils.logger import setup_logging

logger = logging.getLogger(__name__)


@click.group()
@click.version_option()
def main() -> None:
    pass


@main.command()
def config() -> None:
    mgr = ConfigManager()

    if mgr.exists():
        click.echo(f"Configuration file found: {mgr.config_path}")
        if not click.confirm("Overwrite existing configuration?", default=False):
            click.echo("Aborted.")
            return
        mgr.load()
    else:
        click.echo("No configuration file found. Creating a new one.")

    click.echo("\n--- Google Drive ---")
    mgr.get_or_prompt(
        "google_drive.credentials_path", "Path to Google credentials JSON"
    )
    mgr.get_or_prompt("google_drive.token_path", "Path to Google token JSON")

    click.echo("\n--- AWS ---")
    mgr.get_or_prompt("aws.region", "AWS region")
    mgr.get_or_prompt("aws.cognito.user_pool_id", "Cognito User Pool ID")
    mgr.get_or_prompt("aws.cognito.client_id", "Cognito Client ID")
    mgr.get_or_prompt("aws.cognito.identity_pool_id", "Cognito Identity Pool ID")
    mgr.get_or_prompt("aws.cognito.username", "Cognito username (email)")

    click.echo("\n--- Amplify ---")
    mgr.get_or_prompt("aws.amplify.api_endpoint", "AppSync API endpoint URL")
    mgr.get_or_prompt("aws.amplify.storage_bucket", "S3 storage bucket name")

    click.echo("\n--- Prefix disambiguation (optional) ---")
    pd = mgr.config.prefix_disambiguation
    enabled = click.confirm(
        "Enable prefix-based observation disambiguation?",
        default=bool(pd.enabled),
    )
    mgr.set("prefix_disambiguation.enabled", enabled)
    if enabled:
        mgr.get_or_prompt(
            "prefix_disambiguation.discriminator_field",
            "Observation field to disambiguate on (e.g. countryId)",
        )
        click.echo(
            "  Map each filename prefix to the field value it selects "
            "('*' = catch-all)."
        )
        prefixes = dict(pd.prefixes)
        while True:
            prefix = click.prompt(
                "  Filename prefix ('-' = no prefix; Enter to finish)",
                default="",
                show_default=False,
            )
            if prefix == "":
                break
            key = "" if prefix == "-" else prefix
            value = click.prompt(f"  Value for prefix '{key}'")
            prefixes[key] = value
        mgr.set("prefix_disambiguation.prefixes", prefixes)

    try:
        mgr.config.validate()
    except ConfigurationError as e:
        click.echo(f"\nValidation error: {e}", err=True)
        raise SystemExit(1)

    mgr.save()
    click.echo(f"\nConfiguration saved to {mgr.config_path}")


@main.command()
def show() -> None:
    mgr = ConfigManager()

    if not mgr.exists():
        click.echo(f"No configuration file found at {mgr.config_path}")
        click.echo("Run 'amplify-media-migrator config' to create one.")
        raise SystemExit(1)

    try:
        cfg = mgr.load()
    except ConfigurationError as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        raise SystemExit(1)

    click.echo(f"Configuration file: {mgr.config_path}\n")
    click.echo(json.dumps(config_to_dict(cfg), indent=2))


def _load_config() -> ConfigManager:
    mgr = ConfigManager()
    if not mgr.exists():
        click.echo("No configuration found. Run 'amplify-media-migrator config' first.")
        raise SystemExit(1)
    try:
        mgr.load()
    except ConfigurationError as e:
        click.echo(f"Configuration error: {e}", err=True)
        raise SystemExit(1)
    return mgr


def _authenticate_google(cfg: ConfigManager) -> GoogleDriveClient:
    creds_path = Path(cfg.get("google_drive.credentials_path")).expanduser()
    token_path = Path(cfg.get("google_drive.token_path")).expanduser()

    auth_provider = GoogleDriveAuthProvider(
        credentials_path=creds_path,
        token_path=token_path,
    )

    click.echo("Authenticating with Google Drive...")
    if not auth_provider.authenticate():
        click.echo("Google Drive authentication failed.", err=True)
        raise SystemExit(1)

    credentials = auth_provider.get_credentials()
    drive_client = GoogleDriveClient(credentials=credentials)
    drive_client.connect()
    return drive_client


def _authenticate_cognito(cfg: ConfigManager) -> tuple[str, Any]:
    from amplify_auth import CognitoAuthProvider

    cognito = CognitoAuthProvider(
        user_pool_id=cfg.get("aws.cognito.user_pool_id"),
        client_id=cfg.get("aws.cognito.client_id"),
        region=cfg.get("aws.region"),
    )

    username = cfg.get("aws.cognito.username")
    password = click.prompt("Cognito password", hide_input=True)

    click.echo("Authenticating with AWS Cognito...")
    if not cognito.authenticate(username, password):
        click.echo("Cognito authentication failed.", err=True)
        raise SystemExit(1)

    id_token: str = cognito.get_id_token() or ""
    if not id_token:
        click.echo("Failed to obtain ID token.", err=True)
        raise SystemExit(1)

    return id_token, cognito


def _create_engine(
    cfg: ConfigManager,
    drive_client: GoogleDriveClient,
    id_token: str,
    cognito_provider: Any = None,
) -> MigrationEngine:
    migration_cfg = cfg.config.migration

    storage_client = AmplifyStorageClient(
        bucket=cfg.get("aws.amplify.storage_bucket"),
        region=cfg.get("aws.region"),
        identity_pool_id=cfg.get("aws.cognito.identity_pool_id"),
        user_pool_id=cfg.get("aws.cognito.user_pool_id"),
        max_pool_connections=migration_cfg.concurrency,
    )
    storage_client.connect(id_token)

    graphql_client = GraphQLClient(
        api_endpoint=cfg.get("aws.amplify.api_endpoint"),
        region=cfg.get("aws.region"),
    )
    graphql_client.connect(id_token)

    token_manager: Optional[CognitoTokenManager] = None
    if cognito_provider is not None:

        def _refresh_cognito_token() -> Optional[str]:
            # renew_access_token() calls REFRESH_TOKEN_AUTH and updates
            # cognito_client.id_token in place; get_id_token() alone is a
            # cache read and never contacts AWS.
            try:
                if cognito_provider.cognito_client is None:
                    logger.warning(
                        "Cognito client not initialised; cannot refresh token"
                    )
                    return None
                cognito_provider.cognito_client.renew_access_token()
                new_token: Optional[str] = cognito_provider.cognito_client.id_token
                if new_token:
                    cognito_provider._id_token = new_token
                return new_token
            except Exception:
                logger.exception("Cognito token renewal failed")
                return None

        def _on_new_token(t: str) -> None:
            graphql_client.connect(t)
            storage_client.connect(t)

        token_manager = CognitoTokenManager(
            refresh_fn=_refresh_cognito_token,
            on_token=_on_new_token,
        )

    pd_cfg = cfg.config.prefix_disambiguation

    return MigrationEngine(
        drive_client=drive_client,
        storage_client=storage_client,
        graphql_client=graphql_client,
        progress_tracker=ProgressTracker(),
        mapper=FilenameMapper(),
        concurrency=migration_cfg.concurrency,
        retry_attempts=migration_cfg.retry_attempts,
        retry_delay_seconds=migration_cfg.retry_delay_seconds,
        default_media_public=migration_cfg.default_media_public,
        disambiguation_enabled=pd_cfg.enabled,
        discriminator_field=pd_cfg.discriminator_field or None,
        prefix_rules=dict(pd_cfg.prefixes),
        token_manager=token_manager,
        initial_id_token=id_token,
    )


class _LiveLogHandler(logging.Handler):
    """Routes WARNING+ log records above the live region via the rich console."""

    def __init__(self, live: Any) -> None:
        super().__init__(level=logging.WARNING)
        self._live = live
        self.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._live.console.print(self.format(record))
        except Exception:
            self.handleError(record)


def _quiet_stream_handlers(logger: logging.Logger) -> List[logging.Handler]:
    """Silence stderr stream handlers during a live render; return them to restore."""
    muted: List[logging.Handler] = []
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(
            handler, logging.FileHandler
        ):
            handler.setLevel(logging.CRITICAL + 1)
            muted.append(handler)
    return muted


def _run_with_progress(
    coro_fn: Callable[[], Any],
    engine: MigrationEngine,
    desc: str = "Processing",
) -> None:
    from rich.console import Console

    console = Console(stderr=True)
    reporter = LiveReporter()
    engine.set_reporter(reporter)

    if console.is_terminal:
        _run_live(coro_fn, reporter, console)
    else:
        _run_plain(coro_fn, reporter, console)


def _run_live(
    coro_fn: Callable[[], Any],
    reporter: LiveReporter,
    console: Any,
    fps: int = 6,
) -> None:
    from rich.live import Live

    app_logger = logging.getLogger("amplify_media_migrator")
    stop = threading.Event()

    with Live(reporter.render(), console=console, refresh_per_second=fps) as live:
        muted = _quiet_stream_handlers(app_logger)
        log_handler = _LiveLogHandler(live)
        app_logger.addHandler(log_handler)

        def _ticker() -> None:
            while not stop.wait(1.0 / fps):
                reporter.sample()
                live.update(reporter.render())

        thread = threading.Thread(target=_ticker, daemon=True)
        thread.start()
        try:
            asyncio.run(coro_fn())
        finally:
            stop.set()
            thread.join(timeout=2)
            reporter.sample()
            live.update(reporter.render())
            app_logger.removeHandler(log_handler)
            for handler in muted:
                handler.setLevel(logging.NOTSET)


def _run_plain(
    coro_fn: Callable[[], Any],
    reporter: LiveReporter,
    console: Any,
    interval: float = 30.0,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    stop = threading.Event()
    last = clock()

    def _ticker() -> None:
        nonlocal last
        while not stop.wait(1.0):
            reporter.sample()
            now = clock()
            if now - last >= interval:
                last = now
                console.print(reporter.plain_line())

    thread = threading.Thread(target=_ticker, daemon=True)
    thread.start()
    try:
        asyncio.run(coro_fn())
    finally:
        stop.set()
        thread.join(timeout=2)
        console.print(reporter.plain_line())


def _print_summary(summary: dict) -> None:
    click.echo("\n--- Migration Summary ---")
    click.echo(f"  Total files:    {summary['total']}")
    click.echo(f"  Completed:      {summary['completed']}")
    click.echo(f"  Failed:         {summary['failed']}")
    click.echo(f"  Orphan:         {summary['orphan']}")
    click.echo(f"  Needs review:   {summary['needs_review']}")
    click.echo(f"  Partial:        {summary['partial']}")
    click.echo(f"  Pending:        {summary['pending']}")


@main.command()
@click.option("--folder-id", required=True, help="Google Drive folder ID")
def scan(folder_id: str) -> None:
    """Scan Google Drive folder and validate file mappings (dry-run)."""
    cfg = _load_config()
    drive_client = _authenticate_google(cfg)

    click.echo(f"\nScanning folder {folder_id}...")

    engine = MigrationEngine(
        drive_client=drive_client,
        storage_client=AmplifyStorageClient(
            bucket=cfg.get("aws.amplify.storage_bucket"),
            region=cfg.get("aws.region"),
        ),
        graphql_client=GraphQLClient(
            api_endpoint=cfg.get("aws.amplify.api_endpoint"),
            region=cfg.get("aws.region"),
        ),
        progress_tracker=ProgressTracker(),
        mapper=FilenameMapper(),
        concurrency=cfg.config.migration.concurrency,
    )

    try:
        pattern_counts = asyncio.run(engine.scan(folder_id))
    except MigratorError as e:
        click.echo(f"\nError: {e}", err=True)
        raise SystemExit(1)

    total = sum(pattern_counts.values())
    click.echo(f"\nScan complete. {total} files found.\n")
    click.echo("Pattern breakdown:")
    click.echo(f"  Single:    {pattern_counts.get('single', 0)}")
    click.echo(f"  Multiple:  {pattern_counts.get('multiple', 0)}")
    click.echo(f"  Range:     {pattern_counts.get('range', 0)}")
    click.echo(f"  Invalid:   {pattern_counts.get('invalid', 0)}")

    _print_summary(engine.get_summary())


@main.command()
@click.option("--folder-id", required=True, help="Google Drive folder ID")
@click.option("--dry-run", is_flag=True, help="Validate without uploading")
@click.option(
    "--rescan",
    is_flag=True,
    help="Re-list the Drive folder to pick up newly-added files",
)
@click.option(
    "--retry-orphans", is_flag=True, help="Retry files previously marked as orphan"
)
@click.option("--verbose", is_flag=True, help="Enable debug logging")
def migrate(
    folder_id: str,
    dry_run: bool,
    rescan: bool,
    retry_orphans: bool,
    verbose: bool,
) -> None:
    """Run or resume the media migration.

    Safe to re-run: completed files are skipped and previously failed files are
    retried automatically. The first run scans the Drive folder; later runs
    rebuild work from the saved progress file unless --rescan is passed.
    """
    if verbose:
        setup_logging(level="DEBUG")

    cfg = _load_config()
    drive_client = _authenticate_google(cfg)
    id_token, cognito_provider = _authenticate_cognito(cfg)
    engine = _create_engine(cfg, drive_client, id_token, cognito_provider)

    if dry_run:
        click.echo("\n[DRY RUN] No files will be downloaded or uploaded.\n")

    _peek = ProgressTracker()
    if _peek.load(folder_id):
        _s = _peek.get_summary()
        retrying = (
            _s.failed
            + _s.partial
            + _s.downloaded
            + _s.uploaded
            + (_s.orphan if retry_orphans else 0)
        )
        click.echo(
            f"Resuming: {_s.pending} pending  |  {retrying} retrying"
            f"  |  {_s.needs_review} needs-review check  |  {_s.completed} already done"
        )

    click.echo(f"Starting migration for folder {folder_id}...")

    try:
        _run_with_progress(
            lambda: engine.migrate(folder_id, dry_run, retry_orphans, rescan),
            engine,
            desc="Migrating",
        )
    except MigratorError as e:
        click.echo(f"\nError: {e}", err=True)
        raise SystemExit(1)

    _print_summary(engine.get_summary())


@main.command()
@click.option("--folder-id", required=True, help="Google Drive folder ID")
def review(folder_id: str) -> None:
    """Show files that need manual review."""
    tracker = ProgressTracker()
    if not tracker.load(folder_id):
        click.echo(f"No progress file found for folder {folder_id}")
        raise SystemExit(1)

    files = tracker.get_files_by_status(FileStatus.NEEDS_REVIEW)
    if not files:
        click.echo("No files need review.")
        return

    click.echo(f"Files needing review: {len(files)}\n")
    for fp in files:
        click.echo(f"  {fp.filename}")
        if fp.error:
            click.echo(f"    Reason: {fp.error}")


@main.command()
@click.option("--folder-id", required=True, help="Google Drive folder ID")
@click.option(
    "--status",
    type=click.Choice(["needs_review", "orphan", "failed", "partial"]),
    required=True,
    help="Status of files to export",
)
@click.option("--output", required=True, help="Output file path")
def export(folder_id: str, status: str, output: str) -> None:
    """Export files with a given status to a JSON file."""
    tracker = ProgressTracker()
    if not tracker.load(folder_id):
        click.echo(f"No progress file found for folder {folder_id}")
        raise SystemExit(1)

    file_status = FileStatus(status)
    count = tracker.export_to_json(file_status, Path(output))
    click.echo(f"Exported {count} files with status '{status}' to {output}")


@main.command()
@click.option("--folder-id", required=True, help="Google Drive folder ID")
def validate(folder_id: str) -> None:
    """Run pre-flight checks before migration."""
    failed = False

    # 1. Config
    try:
        cfg = _load_config()
        click.echo("[PASS] Configuration loaded")
    except SystemExit:
        click.echo("[FAIL] Configuration")
        raise SystemExit(1)

    # 2. Google Drive authentication
    google_ok = False
    try:
        drive_client = _authenticate_google(cfg)
        click.echo("[PASS] Google Drive authentication")
        google_ok = True
    except SystemExit:
        click.echo("[FAIL] Google Drive authentication")
        failed = True

    # 3. Google Drive folder access
    if google_ok:
        try:
            list(drive_client.list_files(folder_id, recursive=False))
            click.echo("[PASS] Google Drive folder access")
        except Exception as e:
            click.echo(f"[FAIL] Google Drive folder access: {e}")
            failed = True
    else:
        click.echo("[SKIP] Google Drive folder access")

    # 4. Cognito authentication
    cognito_ok = False
    try:
        id_token, _ = _authenticate_cognito(cfg)
        click.echo("[PASS] Cognito authentication")
        cognito_ok = True
    except SystemExit:
        click.echo("[FAIL] Cognito authentication")
        failed = True

    # 5. S3 bucket access
    if cognito_ok:
        try:
            storage_client = AmplifyStorageClient(
                bucket=cfg.get("aws.amplify.storage_bucket"),
                region=cfg.get("aws.region"),
                identity_pool_id=cfg.get("aws.cognito.identity_pool_id"),
                user_pool_id=cfg.get("aws.cognito.user_pool_id"),
            )
            storage_client.connect(id_token)
            storage_client.file_exists("media/__validate_check__")
            click.echo("[PASS] S3 bucket access")
        except Exception as e:
            click.echo(f"[FAIL] S3 bucket access: {e}")
            failed = True
    else:
        click.echo("[SKIP] S3 bucket access")

    # 6. GraphQL endpoint
    if cognito_ok:
        try:
            graphql_client = GraphQLClient(
                api_endpoint=cfg.get("aws.amplify.api_endpoint"),
                region=cfg.get("aws.region"),
            )
            graphql_client.connect(id_token)
            graphql_client.get_observation_by_sequential_id(0)
            click.echo("[PASS] GraphQL endpoint")
        except Exception as e:
            click.echo(f"[FAIL] GraphQL endpoint: {e}")
            failed = True
    else:
        click.echo("[SKIP] GraphQL endpoint")

    if failed:
        click.echo("\nValidation failed.")
        raise SystemExit(1)
    else:
        click.echo("\nAll checks passed.")


@main.command()
@click.option("--folder-id", required=True, help="Google Drive folder ID")
def status(folder_id: str) -> None:
    """Show migration progress for a folder."""
    tracker = ProgressTracker()
    if not tracker.load(folder_id):
        click.echo(f"No progress file found for folder {folder_id}")
        raise SystemExit(1)

    summary = tracker.get_summary()
    total = tracker.total_files or len(tracker.files)
    processed = (
        summary.completed
        + summary.failed
        + summary.orphan
        + summary.needs_review
        + summary.partial
    )

    click.echo(f"Migration status for folder {folder_id}\n")
    click.echo(f"  Total files:    {total}")
    click.echo(f"  Completed:      {summary.completed}")
    click.echo(f"  Failed:         {summary.failed}")
    click.echo(f"  Orphan:         {summary.orphan}")
    click.echo(f"  Needs review:   {summary.needs_review}")
    click.echo(f"  Partial:        {summary.partial}")
    click.echo(f"  Pending:        {summary.pending}")
    click.echo(f"  Downloaded:     {summary.downloaded}")
    click.echo(f"  Uploaded:       {summary.uploaded}")

    if total > 0:
        pct = (processed / total) * 100
        click.echo(f"\n  Progress:       {pct:.1f}%")
    else:
        click.echo(f"\n  Progress:       0.0%")


if __name__ == "__main__":
    main()
