"""Google Drive OAuth2 authentication provider."""

import json
import logging
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


class GoogleDriveAuthProvider:
    """Google Drive OAuth2 authentication provider."""

    def __init__(
        self,
        credentials_path: Path,
        token_path: Path,
    ) -> None:
        self._credentials_path = credentials_path
        self._token_path = token_path
        self._credentials: Optional[Credentials] = None

    def authenticate(self) -> bool:
        """Authenticate with Google Drive via OAuth2 flow."""
        try:
            if self.load_token():
                if self._credentials is not None and self._credentials.valid:
                    logger.info("Using existing valid token")
                    return True

                if self.refresh_if_needed():
                    return True

            return self._run_oauth_flow()

        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            return False

    def get_credentials(self) -> Optional[Credentials]:
        """Return the current credentials."""
        return self._credentials

    def is_authenticated(self) -> bool:
        """Check if currently authenticated with valid credentials."""
        return self._credentials is not None and self._credentials.valid

    def refresh_if_needed(self) -> bool:
        """Refresh the token if expired."""
        try:
            if self._credentials is None:
                return False

            if self._credentials.valid:
                return True

            if not self._credentials.expired or not self._credentials.refresh_token:
                return False

            self._credentials.refresh(Request())
            self.save_token()
            logger.info("Token refreshed successfully")
            return True

        except Exception as e:
            logger.warning(f"Token refresh failed: {e}")
            return False

    def save_token(self) -> None:
        """Save credentials token to disk."""
        if self._credentials is None:
            return

        self._token_path.parent.mkdir(parents=True, exist_ok=True)
        self._token_path.write_text(self._credentials.to_json())
        logger.debug(f"Token saved to {self._token_path}")

    def load_token(self) -> bool:
        """Load credentials token from disk."""
        try:
            if not self._token_path.exists():
                return False

            self._credentials = Credentials.from_authorized_user_file(
                str(self._token_path), SCOPES
            )
            return True

        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.warning(f"Corrupted token file, will re-authenticate: {e}")
            self._credentials = None
            return False

    def _run_oauth_flow(self) -> bool:
        """Run the OAuth2 browser flow."""
        try:
            if not self._credentials_path.exists():
                logger.error(f"Credentials file not found: {self._credentials_path}")
                return False

            flow = InstalledAppFlow.from_client_secrets_file(
                str(self._credentials_path), SCOPES
            )
            self._credentials = flow.run_local_server(port=0)
            self.save_token()
            logger.info("Authentication successful")
            return True

        except Exception as e:
            logger.error(f"OAuth flow failed: {e}")
            return False
