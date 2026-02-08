from typing import Optional, List
from ..sources.google_drive import GoogleDriveClient, DriveFile
from ..targets.amplify_storage import AmplifyStorageClient
from ..targets.graphql_client import GraphQLClient
from .progress import ProgressTracker
from .mapper import FilenameMapper


class MigrationEngine:
    def __init__(
        self,
        drive_client: GoogleDriveClient,
        storage_client: AmplifyStorageClient,
        graphql_client: GraphQLClient,
        progress_tracker: ProgressTracker,
        mapper: FilenameMapper,
        concurrency: int = 10,
        retry_attempts: int = 3,
        retry_delay_seconds: int = 5,
    ) -> None:
        self._drive_client = drive_client
        self._storage_client = storage_client
        self._graphql_client = graphql_client
        self._progress_tracker = progress_tracker
        self._mapper = mapper
        self._concurrency = concurrency
        self._retry_attempts = retry_attempts
        self._retry_delay_seconds = retry_delay_seconds

    async def scan(self, folder_id: str) -> None:
        raise NotImplementedError

    async def migrate(
        self,
        folder_id: str,
        dry_run: bool = False,
        skip_existing: bool = False,
    ) -> None:
        raise NotImplementedError

    async def resume(self) -> None:
        raise NotImplementedError

    async def process_file(
        self,
        file: DriveFile,
        dry_run: bool = False,
        skip_existing: bool = False,
    ) -> None:
        raise NotImplementedError

    def get_summary(self) -> dict:
        raise NotImplementedError
