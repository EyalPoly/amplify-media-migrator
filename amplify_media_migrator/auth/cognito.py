from typing import Optional


class CognitoAuthProvider:
    def __init__(
        self,
        user_pool_id: str,
        client_id: str,
        region: str = "us-east-1",
    ) -> None:
        self._user_pool_id = user_pool_id
        self._client_id = client_id
        self._region = region
        self._id_token: Optional[str] = None
        self._access_token: Optional[str] = None

    def authenticate(self, username: str, password: str) -> bool:
        raise NotImplementedError

    def get_id_token(self) -> Optional[str]:
        return self._id_token

    def get_access_token(self) -> Optional[str]:
        return self._access_token

    def is_authenticated(self) -> bool:
        return self._id_token is not None

    def refresh_tokens(self) -> bool:
        raise NotImplementedError

    def is_admin(self) -> bool:
        raise NotImplementedError
