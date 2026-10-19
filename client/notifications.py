import asyncio
import inspect
import shutil
import unicodedata
from html import escape
from pathlib import Path
from typing import Any, Callable, Optional


# Control, format (bidi overrides, zero-width characters), line and paragraph separator characters.
_REPLACED_CATEGORIES = frozenset({"Cc", "Cf", "Zl", "Zp"})


def _safe(text: str, limit: int = 120) -> str:
    """Notification text comes from the server (paths) and may hold markup or control characters.

    Each run of control, format and separator characters (newlines, ESC and OSC sequences, bidi
    overrides, zero-width characters) becomes one space, then the text is truncated, then the
    markup characters are escaped.
    """
    chars = []
    in_run = False
    for ch in text:
        if unicodedata.category(ch) in _REPLACED_CATEGORIES:
            if not in_run:
                chars.append(" ")
            in_run = True
        else:
            chars.append(ch)
            in_run = False
    text = "".join(chars)
    text = text if len(text) <= limit else text[:limit] + "…"
    return escape(text, quote=False)


async def _run_command(
    cmd: list[str],
    runner: Optional[Callable[..., Any]],
    capture_stdout: bool = False,
) -> tuple[int, str]:
    proc = None
    stdout_data = ""
    returncode = 0
    try:
        if runner:
            res = runner(cmd) if inspect.isfunction(runner) or callable(runner) else runner(*cmd)
            if asyncio.iscoroutine(res):
                res = await res
            if hasattr(res, "communicate") or hasattr(res, "kill") or hasattr(res, "wait"):
                proc = res
            if capture_stdout and hasattr(res, "communicate"):
                stdout_bytes, _ = await res.communicate()
                stdout_data = stdout_bytes.decode() if isinstance(stdout_bytes, bytes) else str(stdout_bytes)
            elif hasattr(res, "wait"):
                returncode = await res.wait()
            elif hasattr(res, "stdout") or hasattr(res, "returncode"):
                stdout_data = getattr(res, "stdout", "")
                returncode = getattr(res, "returncode", 0)
            elif isinstance(res, (tuple, list)):
                if len(res) >= 1:
                    returncode = res[0]
                if len(res) >= 2:
                    stdout_data = res[1]
            elif isinstance(res, str):
                stdout_data = res
        else:
            if capture_stdout:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout_bytes, _ = await proc.communicate()
                stdout_data = stdout_bytes.decode()
                returncode = proc.returncode or 0
            else:
                proc = await asyncio.create_subprocess_exec(*cmd)
                returncode = await proc.wait()
    except asyncio.CancelledError:
        if proc:
            try:
                proc.kill()
            except Exception:
                pass
        raise
    return returncode, str(stdout_data)


async def _invoke_on_resolve(
    on_resolve: Optional[Callable[[str, str], Any]],
    rel_path: str,
    action: str,
) -> None:
    if not on_resolve:
        return
    if inspect.iscoroutinefunction(on_resolve):
        await on_resolve(rel_path, action)
    else:
        cb_res = on_resolve(rel_path, action)
        if asyncio.iscoroutine(cb_res):
            await cb_res


async def handle_conflict_notification(
    workspace: str,
    rel_path: str,
    server_file: Path,
    conflict_file: Path,
    on_resolve: Optional[Callable[[str, str], Any]] = None,
    runner: Optional[Callable[..., Any]] = None,
) -> None:
    """
    Triggers an interactive desktop notification via notify-send for a detected file conflict.
    Allows user to choose between launching Meld, keeping the local conflict version, or keeping the server version.
    """
    cmd = [
        "notify-send",
        "-u", "critical",
        "-a", "SyncMe",
        "-i", "dialog-warning",
        f"SyncMe Conflict: {_safe(server_file.name)}",
        f"Conflict in workspace '{_safe(workspace)}': {_safe(rel_path)}\nServer and local copies have diverged.",
        "-A", "meld=🔍 Resolve with Meld",
        "-A", "local=💾 Keep my local version",
        "-A", "server=☁️ Keep server version",
    ]

    try:
        _, stdout_data = await _run_command(cmd, runner, capture_stdout=True)
        action = stdout_data.strip()

        if action == "meld":
            meld_cmd = ["meld", str(server_file), str(conflict_file)]
            returncode, _ = await _run_command(meld_cmd, runner, capture_stdout=False)
            if returncode == 0:
                await _invoke_on_resolve(on_resolve, rel_path, "meld")
                await asyncio.to_thread(conflict_file.unlink, missing_ok=True)

        elif action == "local":
            await _invoke_on_resolve(on_resolve, rel_path, "local")
            if conflict_file.exists():
                await asyncio.to_thread(shutil.copy2, conflict_file, server_file)
            await asyncio.to_thread(conflict_file.unlink, missing_ok=True)

        elif action == "server":
            await _invoke_on_resolve(on_resolve, rel_path, "server")
            await asyncio.to_thread(conflict_file.unlink, missing_ok=True)

    except asyncio.CancelledError:
        raise
    except Exception as e:
        print(f"[Notifications] Conflict notification error on {rel_path}: {e}", flush=True)
