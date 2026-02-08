from typing import Optional, BinaryIO
from pathlib import Path


class AmplifyStorageClient:
    def __init__(
        self,
        bucket: str,
        region: str = "us-east-1",
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._client: Optional[object] = None

    def connect(self, id_token: str) -> None:
        raise NotImplementedError

    def upload_file(
        self,
        data: bytes,
        key: str,
        content_type: str,
    ) -> str:
        raise NotImplementedError

    def upload_file_multipart(
        self,
        file_path: Path,
        key: str,
        content_type: str,
        chunk_size_mb: int = 8,
    ) -> str:
        raise NotImplementedError

    def file_exists(self, key: str) -> bool:
        raise NotImplementedError

    def get_url(self, key: str) -> str:
        raise NotImplementedError

    def delete_file(self, key: str) -> None:
        raise NotImplementedError
