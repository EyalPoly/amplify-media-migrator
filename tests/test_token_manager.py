import base64
import json
import logging
import time
from typing import Optional
from unittest.mock import MagicMock

import pytest

from amplify_media_migrator.auth.token_manager import (
    CognitoTokenManager,
    REFRESH_BEFORE_EXPIRY_SECONDS,
    _decode_jwt_expiry,
)

pytestmark = pytest.mark.unit


def _make_jwt(exp: int) -> str:
    """Build a minimal fake JWT with the given exp claim."""
    header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
    payload_bytes = json.dumps({"exp": exp, "sub": "test"}).encode()
    payload = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=").decode()
    return f"{header}.{payload}.fakesignature"


class TestDecodeJwtExpiry:
    def test_returns_exp_claim(self) -> None:
        token = _make_jwt(exp=9999999999)
        assert _decode_jwt_expiry(token) == 9999999999

    def test_returns_none_for_malformed_token(self) -> None:
        assert _decode_jwt_expiry("not.a.valid") is None
        assert _decode_jwt_expiry("") is None
        assert _decode_jwt_expiry("only-one-part") is None

    def test_returns_none_when_exp_missing(self) -> None:
        header = base64.urlsafe_b64encode(b'{"alg":"RS256"}').rstrip(b"=").decode()
        payload = base64.urlsafe_b64encode(b'{"sub":"test"}').rstrip(b"=").decode()
        token = f"{header}.{payload}.sig"
        assert _decode_jwt_expiry(token) is None


class TestCognitoTokenManager:
    def test_refresh_fires_when_token_near_expiry(self) -> None:
        soon = int(time.time()) + REFRESH_BEFORE_EXPIRY_SECONDS - 10
        initial_token = _make_jwt(exp=soon)
        new_token = _make_jwt(exp=int(time.time()) + 3600)

        refresh_fn = MagicMock(return_value=new_token)
        on_token = MagicMock()

        manager = CognitoTokenManager(
            refresh_fn=refresh_fn,
            on_token=on_token,
            check_interval_seconds=0.05,
        )
        manager.start(initial_token)
        time.sleep(0.2)
        manager.stop()

        refresh_fn.assert_called()
        on_token.assert_called_with(new_token)

    def test_no_refresh_when_token_has_plenty_of_time(self) -> None:
        far_future = int(time.time()) + 7200
        initial_token = _make_jwt(exp=far_future)

        refresh_fn = MagicMock(return_value=_make_jwt(exp=int(time.time()) + 7200))
        on_token = MagicMock()

        manager = CognitoTokenManager(
            refresh_fn=refresh_fn,
            on_token=on_token,
            check_interval_seconds=0.05,
        )
        manager.start(initial_token)
        time.sleep(0.15)
        manager.stop()

        refresh_fn.assert_not_called()
        on_token.assert_not_called()

    def test_stop_terminates_thread(self) -> None:
        token = _make_jwt(exp=int(time.time()) + 7200)
        manager = CognitoTokenManager(
            refresh_fn=MagicMock(return_value=None),
            on_token=MagicMock(),
            check_interval_seconds=10.0,
        )
        manager.start(token)
        assert manager._thread is not None
        assert manager._thread.is_alive()

        manager.stop()

        assert not manager._thread.is_alive()

    def test_refresh_fn_returning_none_does_not_call_on_token(self) -> None:
        soon = int(time.time()) + REFRESH_BEFORE_EXPIRY_SECONDS - 10
        initial_token = _make_jwt(exp=soon)

        refresh_fn = MagicMock(return_value=None)
        on_token = MagicMock()

        manager = CognitoTokenManager(
            refresh_fn=refresh_fn,
            on_token=on_token,
            check_interval_seconds=0.05,
        )
        manager.start(initial_token)
        time.sleep(0.2)
        manager.stop()

        refresh_fn.assert_called()
        on_token.assert_not_called()

    def test_double_start_does_not_create_second_thread(self) -> None:
        token = _make_jwt(exp=int(time.time()) + 7200)
        manager = CognitoTokenManager(
            refresh_fn=MagicMock(return_value=None),
            on_token=MagicMock(),
            check_interval_seconds=10.0,
        )
        manager.start(token)
        first_thread = manager._thread

        manager.start(token)  # second call — should be a no-op

        assert manager._thread is first_thread
        manager.stop()

    def test_on_token_exception_does_not_kill_thread(self) -> None:
        soon = int(time.time()) + REFRESH_BEFORE_EXPIRY_SECONDS - 10
        initial_token = _make_jwt(exp=soon)
        new_token = _make_jwt(exp=int(time.time()) + 3600)

        refresh_fn = MagicMock(return_value=new_token)
        on_token = MagicMock(side_effect=RuntimeError("connect failed"))

        manager = CognitoTokenManager(
            refresh_fn=refresh_fn,
            on_token=on_token,
            check_interval_seconds=0.05,
        )
        manager.start(initial_token)
        time.sleep(0.2)

        assert manager._thread is not None
        assert manager._thread.is_alive()

        manager.stop()

    def test_decode_failure_logs_warning_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        malformed_token = "not.a.valid.jwt"

        manager = CognitoTokenManager(
            refresh_fn=MagicMock(return_value=None),
            on_token=MagicMock(),
            check_interval_seconds=0.05,
        )

        with caplog.at_level(
            logging.WARNING, logger="amplify_media_migrator.auth.token_manager"
        ):
            manager.start(malformed_token)
            time.sleep(0.2)
            manager.stop()

        warnings = [
            r
            for r in caplog.records
            if "decode" in r.message.lower() or "expiry" in r.message.lower()
        ]
        assert len(warnings) == 1
