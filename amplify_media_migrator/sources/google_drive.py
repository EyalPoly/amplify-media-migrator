import io
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, NoReturn, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from amplify_media_migrator.utils.exceptions import (
    AuthenticationError,
    DownloadError,
    RateLimitError,
)
from amplify_media_migrator.utils.rate_limiter import RateLimiter

logger = logging.getLogger(__name__)

FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


@dataclass
class DriveFile:
    id: str
    name: str
    mime_type: str
    size: int
    parent_id: Optional[str] = None


class GoogleDriveClient:
    def __init__(
        self,
        credentials: Any,
        rate_limiter: Optional[RateLimiter] = None,
    ) -> None:
        self._credentials = credentials
        self._rate_limiter = rate_limiter or RateLimiter()
        self._service: Optional[Any] = None

    def connect(self) -> None:
        self._service = build("drive", "v3", credentials=self._credentials)
        logger.info("Connected to Google Drive API")

    def _ensure_connected(self) -> Any:
        if self._service is None:
            raise DownloadError("Not connected to Google Drive. Call connect() first.")
        return self._service

    def _handle_http_error(
        self, error: HttpError, file_id: Optional[str] = None
    ) -> NoReturn:
        status = error.resp.status

        if status == 429:
            retry_after = error.resp.get("retry-after")
            retry_seconds = float(retry_after) if retry_after else None
            raise RateLimitError(
                f"Google Drive API rate limit exceeded: {error}",
                retry_after=retry_seconds,
            )

        if status in (401, 403):
            raise AuthenticationError(
                f"Google Drive authentication error ({status}): {error}",
                provider="google_drive",
            )

        if status == 404:
            raise DownloadError(
                f"File not found in Google Drive: {error}",
                file_id=file_id,
            )

        raise DownloadError(
            f"Google Drive API error ({status}): {error}",
            file_id=file_id,
        )

    def list_files(
        self,
        folder_id: str,
        recursive: bool = True,
    ) -> Iterator[DriveFile]:
        service = self._ensure_connected()
        query = f"'{folder_id}' in parents and trashed=false"
        page_token: Optional[str] = None

        while True:
            try:
                request_kwargs: Dict[str, Any] = {
                    "q": query,
                    "fields": "nextPageToken, files(id, name, mimeType, size, parents)",
                    "pageSize": 1000,
                }
                if page_token:
                    request_kwargs["pageToken"] = page_token

                response = service.files().list(**request_kwargs).execute()
            except HttpError as e:
                self._handle_http_error(e)

            files = response.get("files", [])

            for file_data in files:
                mime_type = file_data.get("mimeType", "")

                if mime_type == FOLDER_MIME_TYPE:
                    if recursive:
                        yield from self.list_files(file_data["id"], recursive=True)
                    continue

                parents = file_data.get("parents", [])
                yield DriveFile(
                    id=file_data["id"],
                    name=file_data["name"],
                    mime_type=mime_type,
                    size=int(file_data.get("size", 0)),
                    parent_id=parents[0] if parents else None,
                )

            page_token = response.get("nextPageToken")
            if not page_token:
                break

    def download_file(self, file_id: str) -> bytes:
        service = self._ensure_connected()
        try:
            request = service.files().get_media(fileId=file_id)
            buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(buffer, request)

            done = False
            while not done:
                _, done = downloader.next_chunk()

            return buffer.getvalue()
        except HttpError as e:
            self._handle_http_error(e, file_id=file_id)

    def download_file_to_path(self, file_id: str, destination: Path) -> None:
        data = self.download_file(file_id)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)

    def get_file_metadata(self, file_id: str) -> DriveFile:
        service = self._ensure_connected()
        try:
            result = (
                service.files()
                .get(fileId=file_id, fields="id,name,mimeType,size,parents")
                .execute()
            )
        except HttpError as e:
            self._handle_http_error(e, file_id=file_id)

        parents = result.get("parents", [])
        return DriveFile(
            id=result["id"],
            name=result["name"],
            mime_type=result.get("mimeType", ""),
            size=int(result.get("size", 0)),
            parent_id=parents[0] if parents else None,
        )

    def get_folder_name(self, folder_id: str) -> str:
        service = self._ensure_connected()
        try:
            result = service.files().get(fileId=folder_id, fields="name").execute()
        except HttpError as e:
            self._handle_http_error(e, file_id=folder_id)

        name: str = result["name"]
        return name
