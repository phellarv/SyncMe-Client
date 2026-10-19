"""
Tests for desktop conflict notifications via notify-send and external diff tools (e.g., Meld).
Follows TDD Gate 2: assertions derived directly from acceptance criteria.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from client.notifications import handle_conflict_notification


class CommandResult(tuple):
    """
    Flexible mock command execution result supporting:
    - tuple unpacking: `code, stdout = res[:2]` or `code, stdout, stderr = res`
    - attributes: `res.returncode`, `res.stdout`, `res.stderr`
    - process mock: `await res.communicate()`
    """
    def __new__(cls, returncode: int = 0, stdout: str = "", stderr: str = ""):
        return super().__new__(cls, (returncode, stdout, stderr))

    @property
    def returncode(self) -> int:
        return self[0]

    @property
    def stdout(self) -> str:
        return self[1]

    @property
    def stderr(self) -> str:
        return self[2]

    async def communicate(self):
        stdout_bytes = self[1].encode() if isinstance(self[1], str) else self[1]
        stderr_bytes = self[2].encode() if isinstance(self[2], str) else self[2]
        return stdout_bytes, stderr_bytes


# ---------------------------------------------------------------------------
# Criterion 1: Meld Action
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_notification_meld_action(tmp_path: Path):
    """
    Criterion 1: When user selects 'meld':
    - Launches `meld` with `server_file` and `conflict_file`.
    - Upon Meld exit (code 0), `conflict_file` is removed.
    - `on_resolve(rel_path, 'meld')` is invoked if provided.
    """
    server_file = tmp_path / "doc.txt"
    conflict_file = tmp_path / "doc.conflict.12345.txt"
    server_file.write_text("server version content")
    conflict_file.write_text("local conflicting content")

    calls = []

    async def mock_runner(*args, **kwargs):
        cmd = list(args[0]) if len(args) == 1 and isinstance(args[0], (list, tuple)) else list(args)
        calls.append(cmd)
        cmd_str = " ".join(str(c) for c in cmd)
        if "notify-send" in cmd_str:
            return CommandResult(0, "meld\n", "")
        if "meld" in cmd_str:
            return CommandResult(0, "", "")
        return CommandResult(0, "", "")

    on_resolve = MagicMock()

    await handle_conflict_notification(
        workspace="test-workspace",
        rel_path="doc.txt",
        server_file=server_file,
        conflict_file=conflict_file,
        on_resolve=on_resolve,
        runner=mock_runner,
    )

    # Assert notify-send was called
    assert any("notify-send" in " ".join(str(c) for c in call) for call in calls)

    # Assert meld was called with server_file and conflict_file
    meld_calls = [call for call in calls if call and call[0] == "meld"]
    assert len(meld_calls) >= 1
    meld_cmd = [str(c) for c in meld_calls[0]]
    assert str(server_file) in meld_cmd
    assert str(conflict_file) in meld_cmd

    # Assert conflict file removed upon Meld code 0, server file remains
    assert not conflict_file.exists()
    assert server_file.exists()

    # Assert on_resolve was called with rel_path and "meld"
    on_resolve.assert_called_once_with("doc.txt", "meld")


# ---------------------------------------------------------------------------
# Criterion 2: Local Action
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_notification_local_action(tmp_path: Path):
    """
    Criterion 2: When user selects 'local':
    - `conflict_file` overwrites `server_file`.
    - `conflict_file` is removed.
    - `on_resolve(rel_path, 'local')` is invoked if provided.
    """
    server_file = tmp_path / "notes.txt"
    conflict_file = tmp_path / "notes.conflict.999.txt"
    server_file.write_text("old server content")
    conflict_file.write_text("new local content to keep")

    async def mock_runner(*args, **kwargs):
        cmd = list(args[0]) if len(args) == 1 and isinstance(args[0], (list, tuple)) else list(args)
        cmd_str = " ".join(str(c) for c in cmd)
        if "notify-send" in cmd_str:
            return CommandResult(0, "local\n", "")
        return CommandResult(0, "", "")

    on_resolve = MagicMock()

    await handle_conflict_notification(
        workspace="default",
        rel_path="notes.txt",
        server_file=server_file,
        conflict_file=conflict_file,
        on_resolve=on_resolve,
        runner=mock_runner,
    )

    # Assert conflict file overwrote server file
    assert server_file.exists()
    assert server_file.read_text() == "new local content to keep"

    # Assert conflict file removed
    assert not conflict_file.exists()

    # Assert on_resolve invoked
    on_resolve.assert_called_once_with("notes.txt", "local")


@pytest.mark.asyncio
async def test_notification_local_action_retains_conflict_file_if_on_resolve_fails(tmp_path: Path):
    """
    Issue #29: When on_resolve fails (e.g. network error during upload),
    conflict_file must NOT be deleted so local user changes are not permanently lost.
    """
    server_file = tmp_path / "notes.txt"
    conflict_file = tmp_path / "notes.conflict.999.txt"
    server_file.write_text("old server content")
    conflict_file.write_text("new local content to keep")

    async def mock_runner(*args, **kwargs):
        cmd = list(args[0]) if len(args) == 1 and isinstance(args[0], (list, tuple)) else list(args)
        cmd_str = " ".join(str(c) for c in cmd)
        if "notify-send" in cmd_str:
            return CommandResult(0, "local\n", "")
        return CommandResult(0, "", "")

    async def failing_on_resolve(rel_path, action):
        raise ConnectionError("Server upload failed")

    await handle_conflict_notification(
        workspace="default",
        rel_path="notes.txt",
        server_file=server_file,
        conflict_file=conflict_file,
        on_resolve=failing_on_resolve,
        runner=mock_runner,
    )

    # Conflict file must still exist and server_file must not be corrupted because upload failed!
    assert conflict_file.exists()
    assert conflict_file.read_text() == "new local content to keep"
    assert server_file.read_text() == "old server content"


@pytest.mark.asyncio
async def test_notification_meld_action_retains_conflict_file_if_on_resolve_fails(tmp_path: Path):
    """Meld action retains conflict file if on_resolve callback fails."""
    server_file = tmp_path / "notes.txt"
    conflict_file = tmp_path / "notes.conflict.txt"
    server_file.write_text("server content")
    conflict_file.write_text("local content")

    async def mock_runner(*args, **kwargs):
        cmd = list(args[0]) if len(args) == 1 and isinstance(args[0], (list, tuple)) else list(args)
        cmd_str = " ".join(str(c) for c in cmd)
        if "notify-send" in cmd_str:
            return CommandResult(0, "meld\n", "")
        if "meld" in cmd_str:
            return CommandResult(0, "", "")
        return CommandResult(0, "", "")

    async def failing_on_resolve(rel_path, action):
        raise ConnectionError("Server upload failed")

    await handle_conflict_notification(
        workspace="default",
        rel_path="notes.txt",
        server_file=server_file,
        conflict_file=conflict_file,
        on_resolve=failing_on_resolve,
        runner=mock_runner,
    )

    assert conflict_file.exists()


@pytest.mark.asyncio
async def test_notification_server_action_retains_conflict_file_if_on_resolve_fails(tmp_path: Path):
    """Server action retains conflict file if on_resolve callback fails."""
    server_file = tmp_path / "notes.txt"
    conflict_file = tmp_path / "notes.conflict.txt"
    server_file.write_text("server content")
    conflict_file.write_text("local content")

    async def mock_runner(*args, **kwargs):
        cmd = list(args[0]) if len(args) == 1 and isinstance(args[0], (list, tuple)) else list(args)
        cmd_str = " ".join(str(c) for c in cmd)
        if "notify-send" in cmd_str:
            return CommandResult(0, "server\n", "")
        return CommandResult(0, "", "")

    async def failing_on_resolve(rel_path, action):
        raise ConnectionError("Server update failed")

    await handle_conflict_notification(
        workspace="default",
        rel_path="notes.txt",
        server_file=server_file,
        conflict_file=conflict_file,
        on_resolve=failing_on_resolve,
        runner=mock_runner,
    )

    assert conflict_file.exists()


# ---------------------------------------------------------------------------
# Criterion 3: Server Action
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_notification_server_action(tmp_path: Path):
    """
    Criterion 3: When user selects 'server':
    - `server_file` is kept untouched.
    - `conflict_file` is removed.
    - `on_resolve(rel_path, 'server')` is invoked if provided.
    """
    server_file = tmp_path / "data.csv"
    conflict_file = tmp_path / "data.conflict.111.csv"
    server_file.write_text("server authoritative data")
    conflict_file.write_text("local discarded data")

    async def mock_runner(*args, **kwargs):
        cmd = list(args[0]) if len(args) == 1 and isinstance(args[0], (list, tuple)) else list(args)
        cmd_str = " ".join(str(c) for c in cmd)
        if "notify-send" in cmd_str:
            return CommandResult(0, "server\n", "")
        return CommandResult(0, "", "")

    on_resolve = MagicMock()

    await handle_conflict_notification(
        workspace="work",
        rel_path="data.csv",
        server_file=server_file,
        conflict_file=conflict_file,
        on_resolve=on_resolve,
        runner=mock_runner,
    )

    # Assert server file is untouched
    assert server_file.exists()
    assert server_file.read_text() == "server authoritative data"

    # Assert conflict file removed
    assert not conflict_file.exists()

    # Assert on_resolve invoked
    on_resolve.assert_called_once_with("data.csv", "server")


# ---------------------------------------------------------------------------
# Criterion 4: Dismissed or Timeout
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_notification_dismissed_or_timeout(tmp_path: Path):
    """
    Criterion 4: When notification is dismissed without action (empty output / timeout):
    - Both `server_file` and `conflict_file` remain intact on disk.
    - `on_resolve` is not called.
    """
    server_file = tmp_path / "report.pdf"
    conflict_file = tmp_path / "report.conflict.777.pdf"
    server_file.write_text("server version")
    conflict_file.write_text("conflict version")

    async def mock_runner(*args, **kwargs):
        # Empty output simulates dismiss or timeout
        return CommandResult(0, "", "")

    on_resolve = MagicMock()

    await handle_conflict_notification(
        workspace="default",
        rel_path="report.pdf",
        server_file=server_file,
        conflict_file=conflict_file,
        on_resolve=on_resolve,
        runner=mock_runner,
    )

    # Assert both files remain intact
    assert server_file.exists()
    assert server_file.read_text() == "server version"
    assert conflict_file.exists()
    assert conflict_file.read_text() == "conflict version"

    # Assert on_resolve was not called
    on_resolve.assert_not_called()


# ---------------------------------------------------------------------------
# Criterion 5: Headless or Command Not Found
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_notification_headless_or_command_not_found(tmp_path: Path):
    """
    Criterion 5: When notify-send is not installed or fails (FileNotFoundError or non-zero):
    - Does not raise an unhandled exception.
    - Files remain intact.
    - on_resolve is not called.
    """
    server_file = tmp_path / "headless.txt"
    conflict_file = tmp_path / "headless.conflict.333.txt"
    server_file.write_text("server data")
    conflict_file.write_text("local data")

    # Case A: runner raises FileNotFoundError
    async def failing_runner_filenotfound(*args, **kwargs):
        raise FileNotFoundError("notify-send not found on system")

    on_resolve = MagicMock()

    # Must not raise an exception
    await handle_conflict_notification(
        workspace="headless_ws",
        rel_path="headless.txt",
        server_file=server_file,
        conflict_file=conflict_file,
        on_resolve=on_resolve,
        runner=failing_runner_filenotfound,
    )

    assert server_file.exists()
    assert server_file.read_text() == "server data"
    assert conflict_file.exists()
    assert conflict_file.read_text() == "local data"
    on_resolve.assert_not_called()

    # Case B: runner returns non-zero error without output
    async def failing_runner_error(*args, **kwargs):
        return CommandResult(1, "", "cannot open display")

    await handle_conflict_notification(
        workspace="headless_ws",
        rel_path="headless.txt",
        server_file=server_file,
        conflict_file=conflict_file,
        on_resolve=on_resolve,
        runner=failing_runner_error,
    )

    assert server_file.exists()
    assert conflict_file.exists()
    on_resolve.assert_not_called()

    # Case C: default runner with asyncio.create_subprocess_exec raising FileNotFoundError
    with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError("No notify-send")):
        await handle_conflict_notification(
            workspace="headless_ws",
            rel_path="headless.txt",
            server_file=server_file,
            conflict_file=conflict_file,
            on_resolve=on_resolve,
            runner=None,
        )

    assert server_file.exists()
    assert conflict_file.exists()
    on_resolve.assert_not_called()


# ---------------------------------------------------------------------------
# Criterion 6: Subprocess Cancellation & Non-blocking I/O
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_notification_cancellation_kills_notify_process(tmp_path: Path):
    """
    When the notification task is cancelled during notify-send:
    - The subprocess is killed via proc.kill().
    - asyncio.CancelledError is re-raised.
    """
    server_file = tmp_path / "cancel.txt"
    conflict_file = tmp_path / "cancel.conflict.txt"
    server_file.write_text("server")
    conflict_file.write_text("local")

    mock_proc = MagicMock()
    mock_proc.communicate = AsyncMock(side_effect=asyncio.CancelledError)
    mock_proc.kill = MagicMock()

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
        with pytest.raises(asyncio.CancelledError):
            await handle_conflict_notification(
                workspace="ws",
                rel_path="cancel.txt",
                server_file=server_file,
                conflict_file=conflict_file,
                runner=None,
            )

    mock_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_notification_cancellation_kills_meld_process(tmp_path: Path):
    """
    When the notification task is cancelled during Meld execution:
    - The Meld subprocess is killed via proc_meld.kill().
    - asyncio.CancelledError is re-raised.
    """
    server_file = tmp_path / "meld_cancel.txt"
    conflict_file = tmp_path / "meld_cancel.conflict.txt"
    server_file.write_text("server")
    conflict_file.write_text("local")

    mock_notify_proc = MagicMock()
    mock_notify_proc.communicate = AsyncMock(return_value=(b"meld\n", b""))

    mock_meld_proc = MagicMock()
    mock_meld_proc.wait = AsyncMock(side_effect=asyncio.CancelledError)
    mock_meld_proc.kill = MagicMock()

    async def fake_subprocess_exec(*args, **kwargs):
        if args and args[0] == "notify-send":
            return mock_notify_proc
        return mock_meld_proc

    with patch("asyncio.create_subprocess_exec", side_effect=fake_subprocess_exec):
        with pytest.raises(asyncio.CancelledError):
            await handle_conflict_notification(
                workspace="ws",
                rel_path="meld_cancel.txt",
                server_file=server_file,
                conflict_file=conflict_file,
                runner=None,
            )

    mock_meld_proc.kill.assert_called_once()


@pytest.mark.asyncio
async def test_notification_nonblocking_file_io(tmp_path: Path):
    """
    Verify that shutil.copy2 and conflict_file.unlink are executed via asyncio.to_thread.
    """
    server_file = tmp_path / "nonblock.txt"
    conflict_file = tmp_path / "nonblock.conflict.txt"
    server_file.write_text("old")
    conflict_file.write_text("new")

    async def mock_runner(*args, **kwargs):
        return CommandResult(0, "local\n", "")

    real_to_thread = asyncio.to_thread
    with patch("client.notifications.asyncio.to_thread", wraps=real_to_thread) as mock_to_thread:
        await handle_conflict_notification(
            workspace="ws",
            rel_path="nonblock.txt",
            server_file=server_file,
            conflict_file=conflict_file,
            runner=mock_runner,
        )

    # Check to_thread was called for copy2 and unlink
    funcs_called = [call.args[0] for call in mock_to_thread.call_args_list]
    import shutil
    assert shutil.copy2 in funcs_called
    assert conflict_file.unlink in funcs_called
    assert server_file.read_text() == "new"
    assert not conflict_file.exists()

