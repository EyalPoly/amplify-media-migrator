import asyncio
import concurrent.futures
import logging
import random
import threading
from typing import Callable, Dict, List, Optional

from ..auth.token_manager import CognitoTokenManager
from ..sources.google_drive import DriveFile, GoogleDriveClient
from ..targets.amplify_storage import AmplifyStorageClient
from ..targets.graphql_client import GraphQLClient, Observation
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


class MigrationEngine:
    def __init__(
        self,
        drive_client: GoogleDriveClient,
        storage_client: AmplifyStorageClient,
        graphql_client: GraphQLClient,
        progress_tracker: ProgressTracker,
        mapper: FilenameMapper,
        concurrency: int,
        retry_attempts: int = 3,
        retry_delay_seconds: int = 5,
        default_media_public: bool = False,
        large_file_threshold_mb: int = 25,
        token_manager: Optional[CognitoTokenManager] = None,
        initial_id_token: Optional[str] = None,
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
        self._large_file_threshold_bytes: int = large_file_threshold_mb * 1024 * 1024
        self._token_manager = token_manager
        self._initial_id_token = initial_id_token
        self._uploaded_urls: set[str] = set()
        self._on_progress: Optional[Callable[[str, FileStatus], None]] = None
        self._on_total_known: Optional[Callable[[int], None]] = None
        self._on_file_started: Optional[Callable[[str], None]] = None

    def set_progress_callback(
        self, callback: Callable[[str, FileStatus], None]
    ) -> None:
        self._on_progress = callback

    def set_total_callback(self, callback: Callable[[int], None]) -> None:
        self._on_total_known = callback

    def set_file_started_callback(self, callback: Callable[[str], None]) -> None:
        self._on_file_started = callback

    def _populate_url_cache(self) -> None:
        self._uploaded_urls = {
            fp.s3_url
            for fp in self._progress.files.values()
            if fp.s3_url and fp.status == FileStatus.COMPLETED
        }

    def _start_autosave(self, interval: float = 30.0) -> threading.Event:
        stop = threading.Event()

        def _loop() -> None:
            while not stop.wait(interval):
                self._progress.save()

        threading.Thread(target=_loop, daemon=True, name="autosave").start()
        return stop

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

            existing = self._progress.files.get(drive_file.id)
            if existing is not None and existing.status != FileStatus.NEEDS_REVIEW:
                continue

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
        retry_orphans: bool = False,
        rescan: bool = False,
    ) -> None:
        asyncio.get_running_loop().set_default_executor(
            concurrent.futures.ThreadPoolExecutor(max_workers=self._concurrency)
        )
        self._progress.load(folder_id)
        self._populate_url_cache()

        do_scan = (not self._progress.files) or rescan
        if do_scan:
            files_to_process = await self._build_work_with_scan(
                folder_id, dry_run, retry_orphans
            )
        else:
            files_to_process = await self._build_work_from_progress(retry_orphans)

        if self._on_total_known:
            self._on_total_known(len(files_to_process))

        await self._process_files(files_to_process, dry_run)

    def _collect_retryable_ids(self, retry_orphans: bool) -> set[str]:
        ids = (
            set(self._progress.get_failed_file_ids())
            | set(self._progress.get_partial_file_ids())
            | set(self._progress.get_interrupted_file_ids())
        )
        if retry_orphans:
            ids |= set(self._progress.get_orphan_file_ids())
        return ids

    def _requeue_as_pending(self, file_ids: set[str]) -> None:
        for file_id in file_ids:
            fp = self._progress.get_file(file_id)
            if fp:
                self._progress.update_file(
                    file_id=file_id,
                    filename=fp.filename,
                    status=FileStatus.PENDING,
                )

    async def _build_work_with_scan(
        self, folder_id: str, dry_run: bool, retry_orphans: bool
    ) -> List[DriveFile]:
        files = await asyncio.to_thread(
            lambda: list(self._drive_client.list_files(folder_id))
        )
        self._progress.set_total_files(len(files))

        for drive_file in files:
            existing = self._progress.files.get(drive_file.id)
            if existing is not None and existing.status != FileStatus.NEEDS_REVIEW:
                continue
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

        self._requeue_as_pending(self._collect_retryable_ids(retry_orphans))

        if not dry_run:
            self._progress.save()

        pending_ids = set(self._progress.get_pending_file_ids())
        return [f for f in files if f.id in pending_ids]

    async def _build_work_from_progress(self, retry_orphans: bool) -> List[DriveFile]:
        pending_ids = set(self._progress.get_pending_file_ids())
        retryable_ids = self._collect_retryable_ids(retry_orphans)
        needs_review_ids = set(self._progress.get_needs_review_file_ids())

        self._requeue_as_pending(retryable_ids)

        files_to_process: List[DriveFile] = []
        for file_id in pending_ids | retryable_ids:
            fp = self._progress.get_file(file_id)
            if fp is None:
                logger.warning(
                    "No stored progress entry for file %s, skipping", file_id
                )
                continue
            files_to_process.append(
                DriveFile(id=file_id, name=fp.filename, mime_type="", size=0)
            )

        if needs_review_ids:
            logger.info(
                "Checking %d needs_review files for renames...", len(needs_review_ids)
            )
            review_tasks = [
                self._fetch_and_evaluate_needs_review(file_id)
                for file_id in needs_review_ids
            ]
            for drive_file in await asyncio.gather(*review_tasks):
                if drive_file is not None:
                    files_to_process.append(drive_file)

        return files_to_process

    async def _process_files(
        self, files_to_process: List[DriveFile], dry_run: bool
    ) -> None:
        logger.info("Starting processing of %d files...", len(files_to_process))
        if not dry_run and self._token_manager and self._initial_id_token:
            self._token_manager.start(self._initial_id_token)
        _autosave_stop = self._start_autosave() if not dry_run else None
        try:
            await self._run_workers(files_to_process, dry_run)
        finally:
            if _autosave_stop is not None:
                _autosave_stop.set()
                self._progress.save()
            if self._token_manager:
                self._token_manager.stop()
            self._graphql_client.close()

    async def _run_workers(
        self, files_to_process: List[DriveFile], dry_run: bool
    ) -> None:
        queue: "asyncio.Queue[DriveFile]" = asyncio.Queue()
        for file in files_to_process:
            queue.put_nowait(file)

        aborted: List[BaseException] = []

        async def worker() -> None:
            while not aborted:
                try:
                    file = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await self.process_file(file, dry_run)
                except BaseException as exc:  # noqa: BLE001
                    aborted.append(exc)
                    return

        worker_count = min(self._concurrency, len(files_to_process))
        await asyncio.gather(*(worker() for _ in range(worker_count)))

        if aborted:
            raise aborted[0]

    async def _fetch_and_evaluate_needs_review(
        self, file_id: str
    ) -> Optional[DriveFile]:
        try:
            drive_file = await asyncio.to_thread(
                self._drive_client.get_file_metadata, file_id
            )
            parsed = self._mapper.parse(drive_file.name)
            if parsed.pattern != FilenamePattern.INVALID:
                self._progress.update_file(
                    file_id=file_id,
                    filename=drive_file.name,
                    status=FileStatus.PENDING,
                    sequential_ids=parsed.sequential_ids,
                )
                return drive_file
        except MigratorError as e:
            logger.warning(
                "Could not fetch metadata for needs_review file %s: %s", file_id, e
            )
        return None

    async def _lookup_observations(
        self, sequential_ids: List[int]
    ) -> Dict[int, Observation]:
        tasks = [
            asyncio.to_thread(
                self._graphql_client.get_observation_by_sequential_id, sid
            )
            for sid in sequential_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        observations: Dict[int, Observation] = {}
        for sid, result in zip(sequential_ids, results):
            if isinstance(result, BaseException):
                raise result
            if result is not None:
                observations[sid] = result
        return observations

    async def process_file(
        self,
        file: DriveFile,
        dry_run: bool = False,
    ) -> None:
        if self._on_file_started:
            self._on_file_started(file.name)
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
            observations = await self._lookup_observations(parsed.sequential_ids)
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

        if s3_url in self._uploaded_urls:
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

        try:
            existing_media = await asyncio.to_thread(
                self._graphql_client.get_media_by_url, s3_url
            )
        except AuthenticationError:
            raise
        except MigratorError as e:
            logger.warning("Duplicate check failed for %s: %s", file.name, e)
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
            self._notify_progress(file.name, FileStatus.COMPLETED)
            return

        # If this file was previously uploaded (interrupted after S3 upload but
        # before media record creation), reuse the stored S3 URL and skip
        # the download/upload entirely.
        stored = self._progress.get_file(file.id)
        if stored and stored.status == FileStatus.UPLOADED and stored.s3_url:
            s3_url = stored.s3_url
        elif file.size == 0 or file.size > self._large_file_threshold_bytes:
            content_type = get_content_type(parsed.extension)
            try:
                s3_url = await self._stream_upload_with_retry(
                    file, s3_key, content_type
                )
            except AuthenticationError:
                raise
            except MigratorError as e:
                self._mark_failed(file, parsed, f"Stream upload failed: {e}")
                return

            self._progress.update_file(
                file_id=file.id,
                filename=file.name,
                status=FileStatus.UPLOADED,
                sequential_ids=parsed.sequential_ids,
                s3_url=s3_url,
            )
        else:
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
            self._uploaded_urls.add(s3_url)

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

    async def _stream_upload_with_retry(
        self, file: DriveFile, s3_key: str, content_type: str
    ) -> str:
        last_error: Optional[MigratorError] = None
        for attempt in range(self._retry_attempts):
            stream = self._drive_client.open_download_stream(file.id)
            try:
                s3_url: str = await asyncio.to_thread(
                    self._storage_client.upload_file_stream,
                    stream,
                    s3_key,
                    content_type,
                )
                return s3_url
            except AuthenticationError:
                stream.cancel()
                raise
            except RateLimitError as e:
                stream.cancel()
                last_error = e
                delay = e.retry_after or self._retry_delay_seconds * (2**attempt)
                delay += random.uniform(0, 1)
                logger.warning(
                    "Rate limit streaming %s (attempt %d/%d), retrying in %.1fs",
                    file.id,
                    attempt + 1,
                    self._retry_attempts,
                    delay,
                )
                await asyncio.sleep(delay)
            except MigratorError as e:
                stream.cancel()
                last_error = e
                delay = self._retry_delay_seconds * (2**attempt) + random.uniform(0, 1)
                logger.warning(
                    "Error streaming %s (attempt %d/%d): %s, retrying in %.1fs",
                    file.id,
                    attempt + 1,
                    self._retry_attempts,
                    e,
                    delay,
                )
                await asyncio.sleep(delay)
        raise last_error or DownloadError(
            f"Stream upload failed after {self._retry_attempts} attempts",
            file_id=file.id,
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
