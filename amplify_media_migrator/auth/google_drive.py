from pathlib import Path
from typing import Optional, Any


class GoogleDriveAuthProvider:
    def __init__(
        self,
        credentials_path: Path,
        token_path: Path,
    ) -> None:
        self._credentials_path = credentials_path
        self._token_path = token_path
        self._credentials: Optional[Any] = None

    def authenticate(self) -> bool:
        raise NotImplementedError

    def get_credentials(self) -> Optional[Any]:
        return self._credentials

    def is_authenticated(self) -> bool:
        return self._credentials is not None and self._credentials.valid

    def refresh_if_needed(self) -> bool:
        raise NotImplementedError

    def save_token(self) -> None:
        raise NotImplementedError

    def load_token(self) -> bool:
        raise NotImplementedError