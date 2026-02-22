import json
from pathlib import Path

import click

from .config import ConfigManager, ConfigurationError, config_to_dict
from .migration.progress import FileStatus, ProgressTracker


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


@main.command()
@click.option("--folder-id", required=True, help="Google Drive folder ID")
def scan(folder_id: str) -> None:
    """Scan Google Drive folder and validate file mappings (dry-run)."""
    raise NotImplementedError


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
    raise NotImplementedError


@main.command()
def resume() -> None:
    raise NotImplementedError


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


if __name__ == "__main__":
    main()
