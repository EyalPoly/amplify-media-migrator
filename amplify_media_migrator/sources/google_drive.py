from dataclasses import dataclass
from typing import Optional, List, Iterator, Any
from pathlib import Path


@dataclass
class DriveFile:
    id: str
    name: str
    mime_type: str
    size: int
    parent_id: Optional[str] = None


class GoogleDriveClient:
    def __init__(self, credentials: Any) -> None:
        self._credentials = credentials
        self._service: Optional[Any] = None

    def connect(self) -> None:
        raise NotImplementedError

    def list_files(
        self,
        folder_id: str,
        recursive: bool = True,
    ) -> Iterator[DriveFile]:
        raise NotImplementedError

    def download_file(self, file_id: str) -> bytes:
        raise NotImplementedError

    def download_file_to_path(self, file_id: str, destination: Path) -> None:
        raise NotImplementedError

    def get_file_metadata(self, file_id: str) -> DriveFile:
        raise NotImplementedError

    def get_folder_name(self, folder_id: str) -> str:
        raise NotImplementedError
