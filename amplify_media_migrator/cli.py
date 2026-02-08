import click
from typing import Optional


@click.group()
@click.version_option()
def main() -> None:
    pass


@main.command()
def config() -> None:
    raise NotImplementedError


@main.command()
def show() -> None:
    raise NotImplementedError


@main.command()
@click.option("--folder-id", required=True, help="Google Drive folder ID")
def scan(folder_id: str) -> None:
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
def review() -> None:
    raise NotImplementedError


@main.command()
@click.option(
    "--status",
    type=click.Choice(["needs_review", "orphan", "failed", "partial"]),
    required=True,
    help="Status of files to export",
)
@click.option("--output", required=True, help="Output file path")
def export(status: str, output: str) -> None:
    raise NotImplementedError


if __name__ == "__main__":
    main()
