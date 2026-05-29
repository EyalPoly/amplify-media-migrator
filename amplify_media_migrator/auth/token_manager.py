import base64
import json
import logging
import threading
import time
from typing import Callable, Optional

logger = logging.getLogger(__name__)

REFRESH_BEFORE_EXPIRY_SECONDS = 300  # refresh 5 minutes before expiry


def _decode_jwt_expiry(id_token: str) -> Optional[int]:
    """Return the 'exp' claim (Unix timestamp) from a JWT payload, or None on failure."""
    try:
        payload_b64 = id_token.split(".")[1]
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        return int(payload["exp"])
    except Exception:
        return None


class CognitoTokenManager:
    """Background thread that refreshes a Cognito ID token before it expires.

    Calls refresh_fn to obtain a new token, then on_token to distribute it.
    Designed for migrations that outlast the 1-hour Cognito token TTL.
    """

    def __init__(
        self,
        refresh_fn: Callable[[], Optional[str]],
        on_token: Callable[[str], None],
        check_interval_seconds: float = 30.0,
    ) -> None:
        self._refresh_fn = refresh_fn
        self._on_token = on_token
        self._check_interval = check_interval_seconds
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self, current_token: str) -> None:
        # Guard against double-start: if a refresh thread is already running, do nothing.
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            args=(current_token,),
            daemon=True,
            name="token-refresh",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self, current_token: str) -> None:
        warned_decode_failure = False
        while not self._stop_event.is_set():
            expiry = _decode_jwt_expiry(current_token)
            if expiry is None:
                if not warned_decode_failure:
                    logger.warning(
                        "Could not decode token expiry; proactive refresh is disabled"
                    )
                    warned_decode_failure = True
            elif expiry - time.time() < REFRESH_BEFORE_EXPIRY_SECONDS:
                new_token = self._refresh_fn()
                if new_token:
                    current_token = new_token
                    try:
                        self._on_token(new_token)
                        logger.info("Cognito token refreshed proactively")
                    except Exception:
                        logger.exception(
                            "Token distribution callback failed; will retry next interval"
                        )
                else:
                    logger.warning(
                        "Token refresh returned no token; will retry next interval"
                    )
            self._stop_event.wait(self._check_interval)
