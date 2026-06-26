import subprocess
from unittest.mock import MagicMock, patch

import pytest

from amplify_media_migrator.utils.keep_awake import KeepAwake

pytestmark = pytest.mark.unit


@patch("amplify_media_migrator.utils.keep_awake.platform.system", return_value="Darwin")
@patch("amplify_media_migrator.utils.keep_awake.subprocess.Popen")
@patch("amplify_media_migrator.utils.keep_awake.os.getpid", return_value=4242)
def test_macos_spawns_caffeinate(mock_getpid, mock_popen, mock_system):
    handle = MagicMock()
    mock_popen.return_value = handle
    with KeepAwake():
        pass
    args, kwargs = mock_popen.call_args
    assert args[0] == ["caffeinate", "-dimsu", "-w", "4242"]
    assert kwargs["stdout"] == subprocess.DEVNULL
    assert kwargs["stderr"] == subprocess.DEVNULL
    handle.terminate.assert_called_once()
    handle.wait.assert_called_once()


@patch("amplify_media_migrator.utils.keep_awake.platform.system", return_value="Linux")
@patch("amplify_media_migrator.utils.keep_awake.subprocess.Popen")
def test_linux_spawns_systemd_inhibit(mock_popen, mock_system):
    handle = MagicMock()
    mock_popen.return_value = handle
    with KeepAwake(reason="migrating"):
        pass
    cmd = mock_popen.call_args[0][0]
    assert cmd[0] == "systemd-inhibit"
    assert "--what=idle:sleep" in cmd
    assert "--mode=block" in cmd
    assert "--why=migrating" in cmd
    assert cmd[-2:] == ["sleep", "infinity"]
    handle.terminate.assert_called_once()


@patch(
    "amplify_media_migrator.utils.keep_awake.platform.system", return_value="Windows"
)
def test_windows_sets_and_clears_execution_state(mock_system):
    fake_kernel = MagicMock()
    fake_windll = MagicMock(kernel32=fake_kernel)
    with patch("amplify_media_migrator.utils.keep_awake.ctypes") as mock_ctypes:
        mock_ctypes.windll = fake_windll
        with KeepAwake():
            pass
    first_flags = fake_kernel.SetThreadExecutionState.call_args_list[0][0][0]
    last_flags = fake_kernel.SetThreadExecutionState.call_args_list[-1][0][0]
    assert first_flags == (0x80000000 | 0x00000001 | 0x00000002)
    assert last_flags == 0x80000000


@patch("amplify_media_migrator.utils.keep_awake.platform.system", return_value="Linux")
@patch(
    "amplify_media_migrator.utils.keep_awake.subprocess.Popen",
    side_effect=FileNotFoundError("systemd-inhibit not found"),
)
def test_start_failure_warns_and_continues(mock_popen, mock_system, caplog):
    ran = False
    with KeepAwake():
        ran = True
    assert ran is True
    assert any("sleep prevention" in r.message.lower() for r in caplog.records)


@patch("amplify_media_migrator.utils.keep_awake.platform.system", return_value="Darwin")
@patch("amplify_media_migrator.utils.keep_awake.subprocess.Popen")
def test_handle_released_even_when_block_raises(mock_popen, mock_system):
    handle = MagicMock()
    mock_popen.return_value = handle
    with pytest.raises(ValueError):
        with KeepAwake():
            raise ValueError("boom")
    handle.terminate.assert_called_once()


@patch("amplify_media_migrator.utils.keep_awake.platform.system", return_value="SunOS")
def test_unsupported_platform_warns_and_continues(mock_system, caplog):
    with KeepAwake():
        pass
    assert any("sleep prevention" in r.message.lower() for r in caplog.records)
