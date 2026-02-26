import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Callable

import click

from .auth.google_drive import GoogleDriveAuthProvider
from .config import ConfigManager, ConfigurationError, config_to_dict
from .migration.engine import MigrationEngine
from .migration.mapper import FilenameMapper
from .migration.progress import FileStatus, ProgressTracker
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
    mgr.get_or_prompt("google_drive.folder_id", "Google Drive folder ID")
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


def _authenticate_cognito(cfg: ConfigManager) -> str:
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

    return id_token


def _create_engine(
    cfg: ConfigManager,
    drive_client: GoogleDriveClient,
    id_token: str,
    concurrency: int = 10,
) -> MigrationEngine:
    storage_client = AmplifyStorageClient(
        bucket=cfg.get("aws.amplify.storage_bucket"),
        region=cfg.get("aws.region"),
        identity_pool_id=cfg.get("aws.cognito.identity_pool_id"),
        user_pool_id=cfg.get("aws.cognito.user_pool_id"),
    )
    storage_client.connect(id_token)

    graphql_client = GraphQLClient(
        api_endpoint=cfg.get("aws.amplify.api_endpoint"),
        region=cfg.get("aws.region"),
    )
    graphql_client.connect(id_token)

    migration_cfg = cfg.config.migration

    return MigrationEngine(
        drive_client=drive_client,
        storage_client=storage_client,
        graphql_client=graphql_client,
        progress_tracker=ProgressTracker(),
        mapper=FilenameMapper(),
        concurrency=concurrency,
        retry_attempts=migration_cfg.retry_attempts,
        retry_delay_seconds=migration_cfg.retry_delay_seconds,
        default_media_public=migration_cfg.default_media_public,
    )


def _run_with_progress(
    coro_fn: Callable[[], Any],
    engine: MigrationEngine,
    desc: str = "Processing",
) -> None:
    try:
        from tqdm import tqdm

        progress_bar = tqdm(desc=desc, unit="file")

        def on_progress(filename: str, status: FileStatus) -> None:
            progress_bar.update(1)
            progress_bar.set_postfix_str(f"{filename}: {status.value}")

        engine.set_progress_callback(on_progress)

        try:
            asyncio.run(coro_fn())
        finally:
            progress_bar.close()

    except ImportError:
        asyncio.run(coro_fn())


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
    )

    pattern_counts = asyncio.run(engine.scan(folder_id))

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
@click.option("--concurrency", default=10, help="Number of parallel workers")
@click.option("--dry-run", is_flag=True, help="Validate without uploading")
@click.option(
    "--skip-existing", is_flag=True, help="Skip files with existing Media records"
)
@click.option("--verbose", is_flag=True, help="Enable debug logging")
def migrate(
    folder_id: str,
    concurrency: int,
    dry_run: bool,
    skip_existing: bool,
    verbose: bool,
) -> None:
    """Run the full media migration."""
    if verbose:
        setup_logging(level="DEBUG")

    cfg = _load_config()
    drive_client = _authenticate_google(cfg)
    id_token = _authenticate_cognito(cfg)
    engine = _create_engine(cfg, drive_client, id_token, concurrency)

    if dry_run:
        click.echo("\n[DRY RUN] No files will be downloaded or uploaded.\n")

    click.echo(f"Starting migration for folder {folder_id}...")

    _run_with_progress(
        lambda: engine.migrate(folder_id, dry_run, skip_existing),
        engine,
        desc="Migrating",
    )

    _print_summary(engine.get_summary())


@main.command()
@click.option("--folder-id", required=True, help="Google Drive folder ID")
@click.option("--concurrency", default=10, help="Number of parallel workers")
@click.option("--dry-run", is_flag=True, help="Validate without uploading")
@click.option(
    "--skip-existing", is_flag=True, help="Skip files with existing Media records"
)
@click.option("--verbose", is_flag=True, help="Enable debug logging")
def resume(
    folder_id: str,
    concurrency: int,
    dry_run: bool,
    skip_existing: bool,
    verbose: bool,
) -> None:
    """Resume an interrupted migration."""
    if verbose:
        setup_logging(level="DEBUG")

    cfg = _load_config()
    drive_client = _authenticate_google(cfg)
    id_token = _authenticate_cognito(cfg)
    engine = _create_engine(cfg, drive_client, id_token, concurrency)

    click.echo(f"Resuming migration for folder {folder_id}...")

    _run_with_progress(
        lambda: engine.resume(folder_id, dry_run, skip_existing),
        engine,
        desc="Resuming",
    )

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
        id_token = _authenticate_cognito(cfg)
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
