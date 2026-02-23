import asyncio
import logging
import random
from typing import Callable, Dict, List, Optional

from ..sources.google_drive import DriveFile, GoogleDriveClient
from ..targets.amplify_storage import AmplifyStorageClient
from ..targets.graphql_client import GraphQLClient
from ..utils.exceptions import (
    AuthenticationError,
    DownloadError,
    MigratorError,
    RateLimitError,
)
from ..utils.media import get_content_type, get_media_type
from .mapper import FilenameMapper, FilenamePattern, ParsedFilename
from .progress import FileStatus, ProgressTracker

logger = logging.getLogger(__name__)

SAVE_INTERVAL = 50


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
        default_media_public: bool = False,
    ) -> None:
        self._drive_client = drive_client
        self._storage_client = storage_client
        self._graphql_client = graphql_client
        self._progress = progress_tracker
        self._mapper = mapper
        self._concurrency = concurrency
        self._retry_attempts = retry_attempts
        self._retry_delay_seconds = retry_delay_seconds
        self._default_media_public = default_media_public
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._save_lock: Optional[asyncio.Lock] = None
        self._processed_count = 0
        self._on_progress: Optional[Callable[[str, FileStatus], None]] = None

    def set_progress_callback(
        self, callback: Callable[[str, FileStatus], None]
    ) -> None:
        self._on_progress = callback

    def _get_semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._concurrency)
        return self._semaphore

    def _get_save_lock(self) -> asyncio.Lock:
        if self._save_lock is None:
            self._save_lock = asyncio.Lock()
        return self._save_lock

    def _reset_run_state(self) -> None:
        self._processed_count = 0
        self._semaphore = None
        self._save_lock = None

    async def scan(self, folder_id: str) -> Dict[str, int]:
        self._progress.load(folder_id)

        files = await asyncio.to_thread(
            lambda: list(self._drive_client.list_files(folder_id))
        )

        self._progress.set_total_files(len(files))

        pattern_counts: Dict[str, int] = {p.value: 0 for p in FilenamePattern}

        for drive_file in files:
            parsed = self._mapper.parse(drive_file.name)
            pattern_counts[parsed.pattern.value] += 1

            if drive_file.id not in self._progress.files:
                if parsed.pattern == FilenamePattern.INVALID:
                    self._progress.update_file(
                        file_id=drive_file.id,
                        filename=drive_file.name,
                        status=FileStatus.NEEDS_REVIEW,
                        error=parsed.error,
                    )
                else:
                    self._progress.update_file(
                        file_id=drive_file.id,
                        filename=drive_file.name,
                        status=FileStatus.PENDING,
                        sequential_ids=parsed.sequential_ids,
                    )

        self._progress.save()
        return pattern_counts

    async def migrate(
        self,
        folder_id: str,
        dry_run: bool = False,
        skip_existing: bool = False,
    ) -> None:
        self._reset_run_state()
        self._progress.load(folder_id)

        files = await asyncio.to_thread(
            lambda: list(self._drive_client.list_files(folder_id))
        )
        self._progress.set_total_files(len(files))

        for drive_file in files:
            if drive_file.id not in self._progress.files:
                parsed = self._mapper.parse(drive_file.name)
                if parsed.pattern == FilenamePattern.INVALID:
                    self._progress.update_file(
                        file_id=drive_file.id,
                        filename=drive_file.name,
                        status=FileStatus.NEEDS_REVIEW,
                        error=parsed.error,
                    )
                else:
                    self._progress.update_file(
                        file_id=drive_file.id,
                        filename=drive_file.name,
                        status=FileStatus.PENDING,
                        sequential_ids=parsed.sequential_ids,
                    )

        self._progress.save()

        pending_ids = set(self._progress.get_pending_file_ids())
        files_to_process = [f for f in files if f.id in pending_ids]

        self._processed_count = 0
        tasks = [
            self._process_with_semaphore(f, dry_run, skip_existing)
            for f in files_to_process
        ]
        await asyncio.gather(*tasks)

        self._progress.save()

    async def resume(
        self,
        folder_id: str,
        dry_run: bool = False,
        skip_existing: bool = False,
    ) -> None:
        self._reset_run_state()
        if not self._progress.load(folder_id):
            raise MigratorError(f"No progress file found for folder {folder_id}")

        pending_ids = set(self._progress.get_pending_file_ids())
        failed_ids = set(self._progress.get_failed_file_ids())
        partial_ids = set(self._progress.get_partial_file_ids())
        retryable_ids = failed_ids | partial_ids
        file_ids_to_process = pending_ids | retryable_ids

        if not file_ids_to_process:
            logger.info("No files to resume")
            return

        files_to_process: List[DriveFile] = []
        for file_id in file_ids_to_process:
            try:
                drive_file = await asyncio.to_thread(
                    self._drive_client.get_file_metadata, file_id
                )
                files_to_process.append(drive_file)
            except MigratorError as e:
                logger.warning("Could not fetch metadata for %s: %s", file_id, e)
                fp = self._progress.get_file(file_id)
                filename = fp.filename if fp else file_id
                self._progress.update_file(
                    file_id=file_id,
                    filename=filename,
                    status=FileStatus.FAILED,
                    error=f"Could not fetch file metadata: {e}",
                )

        fetched_ids = {f.id for f in files_to_process}
        for file_id in retryable_ids:
            if file_id in fetched_ids:
                fp = self._progress.get_file(file_id)
                if fp:
                    self._progress.update_file(
                        file_id=file_id,
                        filename=fp.filename,
                        status=FileStatus.PENDING,
                    )

        self._processed_count = 0
        tasks = [
            self._process_with_semaphore(f, dry_run, skip_existing)
            for f in files_to_process
        ]
        await asyncio.gather(*tasks)

        self._progress.save()

    async def _process_with_semaphore(
        self,
        file: DriveFile,
        dry_run: bool,
        skip_existing: bool,
    ) -> None:
        async with self._get_semaphore():
            await self.process_file(file, dry_run, skip_existing)
            async with self._get_save_lock():
                self._processed_count += 1
                if self._processed_count % SAVE_INTERVAL == 0:
                    self._progress.save()

    async def process_file(
        self,
        file: DriveFile,
        dry_run: bool = False,
        skip_existing: bool = False,
    ) -> None:
        parsed = self._mapper.parse(file.name)

        if parsed.pattern == FilenamePattern.INVALID:
            self._progress.update_file(
                file_id=file.id,
                filename=file.name,
                status=FileStatus.NEEDS_REVIEW,
                error=parsed.error,
            )
            self._notify_progress(file.name, FileStatus.NEEDS_REVIEW)
            return

        try:
            observations = await asyncio.to_thread(
                self._graphql_client.get_observations_by_sequential_ids,
                parsed.sequential_ids,
            )
        except AuthenticationError:
            raise
        except MigratorError as e:
            self._mark_failed(file, parsed, f"Observation query failed: {e}")
            return

        if not observations:
            self._progress.update_file(
                file_id=file.id,
                filename=file.name,
                status=FileStatus.ORPHAN,
                sequential_ids=parsed.sequential_ids,
                error="No matching observations found",
            )
            self._notify_progress(file.name, FileStatus.ORPHAN)
            return

        first_obs = next(iter(observations.values()))
        s3_key = self._mapper.build_s3_key(first_obs.id, file.name)
        s3_url = self._storage_client.get_url(s3_key)

        if skip_existing:
            try:
                existing_media = await asyncio.to_thread(
                    self._graphql_client.get_media_by_url, s3_url
                )
            except AuthenticationError:
                raise
            except MigratorError as e:
                logger.warning("Skip-existing check failed for %s: %s", file.name, e)
                existing_media = None

            if existing_media:
                self._progress.update_file(
                    file_id=file.id,
                    filename=file.name,
                    status=FileStatus.COMPLETED,
                    sequential_ids=parsed.sequential_ids,
                    observation_ids=[obs.id for obs in observations.values()],
                    s3_url=s3_url,
                )
                self._notify_progress(file.name, FileStatus.COMPLETED)
                return

        if dry_run:
            self._progress.update_file(
                file_id=file.id,
                filename=file.name,
                status=FileStatus.COMPLETED,
                sequential_ids=parsed.sequential_ids,
                observation_ids=[obs.id for obs in observations.values()],
            )
            self._notify_progress(file.name, FileStatus.COMPLETED)
            return

        try:
            data = await self._download_with_retry(file.id)
        except AuthenticationError:
            raise
        except MigratorError as e:
            self._mark_failed(file, parsed, f"Download failed: {e}")
            return

        self._progress.update_file(
            file_id=file.id,
            filename=file.name,
            status=FileStatus.DOWNLOADED,
            sequential_ids=parsed.sequential_ids,
        )

        content_type = get_content_type(parsed.extension)
        try:
            s3_url = await asyncio.to_thread(
                self._storage_client.upload_file,
                data,
                s3_key,
                content_type,
            )
        except AuthenticationError:
            raise
        except MigratorError as e:
            self._mark_failed(file, parsed, f"Upload failed: {e}")
            return

        self._progress.update_file(
            file_id=file.id,
            filename=file.name,
            status=FileStatus.UPLOADED,
            sequential_ids=parsed.sequential_ids,
            s3_url=s3_url,
        )

        media_type = get_media_type(parsed.extension)
        media_ids: List[str] = []
        observation_ids: List[str] = []
        failed_seq_ids: List[int] = []

        for seq_id, obs in observations.items():
            try:
                media = await asyncio.to_thread(
                    self._graphql_client.create_media,
                    s3_url,
                    obs.id,
                    media_type,
                    self._default_media_public,
                )
                media_ids.append(media.id)
                observation_ids.append(obs.id)
            except AuthenticationError:
                raise
            except MigratorError as e:
                logger.warning(
                    "Failed to create media for seq_id=%d, obs=%s: %s",
                    seq_id,
                    obs.id,
                    e,
                )
                failed_seq_ids.append(seq_id)

        if not media_ids:
            status = FileStatus.FAILED
            error = "Failed to create any Media records"
        elif failed_seq_ids:
            status = FileStatus.PARTIAL
            error = f"Failed for sequential IDs: {failed_seq_ids}"
        else:
            status = FileStatus.COMPLETED
            error = None

        self._progress.update_file(
            file_id=file.id,
            filename=file.name,
            status=status,
            sequential_ids=parsed.sequential_ids,
            observation_ids=observation_ids,
            s3_url=s3_url,
            media_ids=media_ids,
            error=error,
        )
        self._notify_progress(file.name, status)

    async def _download_with_retry(self, file_id: str) -> bytes:
        last_error: Optional[MigratorError] = None
        for attempt in range(self._retry_attempts):
            try:
                data: bytes = await asyncio.to_thread(
                    self._drive_client.download_file, file_id
                )
                return data
            except RateLimitError as e:
                last_error = e
                delay = e.retry_after or self._retry_delay_seconds * (2**attempt)
                delay += random.uniform(0, 1)
                logger.warning(
                    "Rate limit downloading %s (attempt %d/%d), retrying in %.1fs",
                    file_id,
                    attempt + 1,
                    self._retry_attempts,
                    delay,
                )
                await asyncio.sleep(delay)
            except (DownloadError,) as e:
                last_error = e
                delay = self._retry_delay_seconds * (2**attempt) + random.uniform(0, 1)
                logger.warning(
                    "Error downloading %s (attempt %d/%d): %s, retrying in %.1fs",
                    file_id,
                    attempt + 1,
                    self._retry_attempts,
                    e,
                    delay,
                )
                await asyncio.sleep(delay)

        raise last_error or DownloadError(
            f"Download failed after {self._retry_attempts} attempts",
            file_id=file_id,
        )

    def _mark_failed(self, file: DriveFile, parsed: ParsedFilename, error: str) -> None:
        self._progress.update_file(
            file_id=file.id,
            filename=file.name,
            status=FileStatus.FAILED,
            sequential_ids=parsed.sequential_ids,
            error=error,
        )
        self._notify_progress(file.name, FileStatus.FAILED)

    def _notify_progress(self, filename: str, status: FileStatus) -> None:
        if self._on_progress:
            self._on_progress(filename, status)

    def get_summary(self) -> Dict[str, int]:
        summary = self._progress.get_summary()
        return {
            "total": self._progress.total_files,
            "pending": summary.pending,
            "downloaded": summary.downloaded,
            "uploaded": summary.uploaded,
            "completed": summary.completed,
            "failed": summary.failed,
            "orphan": summary.orphan,
            "needs_review": summary.needs_review,
            "partial": summary.partial,
        }
