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
    """Refreshes a Cognito ID token (and downstream credentials) before expiry.

    Calls refresh_fn to obtain a new token, then on_token to distribute it.
    Designed for migrations that outlast the 1-hour Cognito token TTL.

    Proactive refresh fires on whichever clock expires first: the JWT ID token
    or, when extra_expiry_fn is supplied, the downstream credentials it reports
    (e.g. the Identity Pool S3 credentials, which have their own ~1h lifetime).
    force_refresh() exposes the same refresh for reactive use when an expired
    token is observed mid-request.
    """

    def __init__(
        self,
        refresh_fn: Callable[[], Optional[str]],
        on_token: Callable[[str], None],
        check_interval_seconds: float = 30.0,
        extra_expiry_fn: Optional[Callable[[], Optional[float]]] = None,
    ) -> None:
        self._refresh_fn = refresh_fn
        self._on_token = on_token
        self._check_interval = check_interval_seconds
        self._extra_expiry_fn = extra_expiry_fn
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._current_token: Optional[str] = None

    def start(self, current_token: str) -> None:
        # Guard against double-start: if a refresh thread is already running, do nothing.
        if self._thread and self._thread.is_alive():
            return
        self._current_token = current_token
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="token-refresh",
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def force_refresh(self) -> bool:
        """Refresh and redistribute the token synchronously. Returns success."""
        return self._do_refresh()

    def _effective_expiry(self, jwt_expiry: Optional[int]) -> Optional[float]:
        candidates = []
        if jwt_expiry is not None:
            candidates.append(float(jwt_expiry))
        if self._extra_expiry_fn is not None:
            try:
                extra = self._extra_expiry_fn()
            except Exception:
                logger.exception("Credential expiry callback failed")
                extra = None
            if extra is not None:
                candidates.append(extra)
        return min(candidates) if candidates else None

    def _credentials_fresh(self) -> bool:
        """True if downstream credentials are already comfortably valid.

        Lets a concurrent burst of force_refresh() calls collapse to a single
        real refresh: the first caller renews, the rest observe fresh creds.
        """
        if self._extra_expiry_fn is None:
            return False
        try:
            expiry = self._extra_expiry_fn()
        except Exception:
            return False
        return (
            expiry is not None and expiry - time.time() >= REFRESH_BEFORE_EXPIRY_SECONDS
        )

    def _do_refresh(self) -> bool:
        with self._lock:
            if self._credentials_fresh():
                return True
            new_token = self._refresh_fn()
            if not new_token:
                logger.warning(
                    "Token refresh returned no token; will retry next interval"
                )
                return False
            self._current_token = new_token
            try:
                self._on_token(new_token)
                logger.info("Cognito token refreshed")
                return True
            except Exception:
                logger.exception(
                    "Token distribution callback failed; will retry next interval"
                )
                return False

    def _run(self) -> None:
        warned_decode_failure = False
        while not self._stop_event.is_set():
            with self._lock:
                token = self._current_token
            jwt_expiry = _decode_jwt_expiry(token) if token else None
            if jwt_expiry is None and self._extra_expiry_fn is None:
                if not warned_decode_failure:
                    logger.warning(
                        "Could not decode token expiry; proactive refresh is disabled"
                    )
                    warned_decode_failure = True
            else:
                expiry = self._effective_expiry(jwt_expiry)
                if (
                    expiry is not None
                    and expiry - time.time() < REFRESH_BEFORE_EXPIRY_SECONDS
                ):
                    self._do_refresh()
            self._stop_event.wait(self._check_interval)
