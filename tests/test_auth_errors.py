"""Only HTTP 401 from the server is fatal (exit code 78, no systemd restart); other HTTP errors are logged.

A revoked key used to crash the daemon with a traceback and make systemd restart it every five
seconds, while upload errors other than 409 were silently ignored. The server answers 401 (never
403) for a bad key, so a 403 is a proxy, WAF or load balancer deny rule: it must not stop the
daemon for good, it is handled like any other non-auth HTTP error.
"""
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from click.testing import CliRunner

import client.cli as cli_module
from client.config import ClientConfig, FileConfig, ServerConfig
from client.db import LocalDB
from client.engine import SyncEngine
from client.errors import EXIT_AUTH, is_auth_error
from client.file_engine import FileGroupEngine

KINDS = ["dir", "file"]
KEY = "SECRETBODY" + "k" * 33
UNIT_FILE = Path(__file__).resolve().parent.parent / "systemd" / "syncme-client.service"


def http_error(status):
    request = httpx.Request("GET", "https://x")
    return httpx.HTTPStatusError(f"HTTP {status}", request=request, response=httpx.Response(status, request=request))


class Harness:
    """The same scenario for SyncEngine ("dir") and FileGroupEngine ("file"): files a.txt, b.txt."""

    def __init__(self, kind, tmp_path, names=("a.txt",)):
        self.api = AsyncMock()
        self.api.get_state.return_value = {"files": [], "last_revision": 1}
        if kind == "dir":
            self.root = tmp_path / "sync"
            self.root.mkdir()
            self.db = LocalDB(self.root / ".syncme" / "local_state.db")
            self.engine = SyncEngine(self.root, self.db, self.api)
        else:
            self.root = tmp_path / "files"
            self.root.mkdir()
            self.db = LocalDB(tmp_path / "state" / "ws.db")
            confs = [FileConfig(local_path=self.root / n, workspace="ws", remote_path=n) for n in names]
            self.engine = FileGroupEngine("ws", confs, self.db, self.api, ServerConfig())
        self.db.set_initialized(True)

    def local(self, name="a.txt"):
        return self.root / name

    def track(self, name, revision=1):
        """Pretend a.txt was synced before (the DB knows it)."""
        self.db.upsert_file(name, "h" * 64, 1, 1.0, revision, False)

    def remote(self, *entries, rev=3):
        self.api.get_state.return_value = {"files": list(entries), "last_revision": rev}


def entry(path="a.txt", revision=2, sha="0" * 64):
    return {"path": path, "revision": revision, "is_deleted": False, "sha256": sha}


# --- is_auth_error ----------------------------------------------------------------------------


def test_is_auth_error_true_for_401():
    assert is_auth_error(http_error(401)) is True


def test_is_auth_error_false_for_403():
    """403 comes from a proxy, WAF or HAProxy deny rule, never from the server for a bad key."""
    assert is_auth_error(http_error(403)) is False


@pytest.mark.parametrize("exc", [http_error(404), http_error(409), http_error(500), ValueError("401"), httpx.ConnectError("x")])
def test_is_auth_error_false_for_everything_else(exc):
    assert is_auth_error(exc) is False


def test_the_unused_auth_error_class_is_gone():
    import client.errors

    assert not hasattr(client.errors, "AuthError")


def test_exit_code_is_78():
    assert EXIT_AUTH == 78


# --- reconcile: auth errors propagate ---------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_get_state_auth_error_propagates(kind, tmp_path):
    h = Harness(kind, tmp_path)
    h.api.get_state.side_effect = http_error(401)
    with pytest.raises(httpx.HTTPStatusError) as exc:
        await h.engine.reconcile()
    assert exc.value.response.status_code == 401


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_local_deletion_auth_error_propagates(kind, tmp_path):
    h = Harness(kind, tmp_path)
    h.track("a.txt")  # tracked, but the local file is gone
    h.api.delete_file.side_effect = http_error(401)
    with pytest.raises(httpx.HTTPStatusError):
        await h.engine.reconcile()
    assert h.db.get_file("a.txt") is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_upload_auth_error_propagates(kind, tmp_path):
    h = Harness(kind, tmp_path)
    h.local().write_text("new")
    h.api.upload_file.side_effect = http_error(401)
    with pytest.raises(httpx.HTTPStatusError):
        await h.engine.reconcile()
    assert h.db.get_file("a.txt") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_download_auth_error_propagates(kind, tmp_path):
    h = Harness(kind, tmp_path)
    h.remote(entry())
    h.api.download_file.side_effect = http_error(401)
    with pytest.raises(httpx.HTTPStatusError):
        await h.engine.reconcile()
    assert h.db.get_last_server_revision() == 0  # not advanced: the entry is fetched again after the key is fixed


# --- initial_sync: auth errors propagate ------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_initial_sync_get_state_auth_error_propagates(kind, tmp_path):
    h = Harness(kind, tmp_path)
    h.api.get_state.side_effect = http_error(401)
    with pytest.raises(httpx.HTTPStatusError):
        await h.engine.initial_sync()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("scenario", ["upload", "download", "conflict-server", "conflict-local", "conflict-both"])
async def test_initial_sync_auth_error_propagates(kind, scenario, tmp_path):
    h = Harness(kind, tmp_path)
    if scenario == "upload":
        h.local().write_text("local only")
    elif scenario == "download":
        h.remote(entry())
    else:
        h.local().write_text("local")
        h.remote(entry())  # sha256 differs from the local file: a conflict
    h.api.upload_file.side_effect = http_error(401)
    h.api.download_file.side_effect = http_error(401)
    choice = scenario.split("-")[1] if scenario.startswith("conflict-") else "ask"
    with pytest.raises(httpx.HTTPStatusError):
        await h.engine.initial_sync(lambda rel, s_file, l_file: choice)


# --- other HTTP errors ------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_upload_409_is_silent(kind, tmp_path, capsys):
    h = Harness(kind, tmp_path)
    h.local().write_text("new")
    h.api.upload_file.side_effect = http_error(409)
    await h.engine.reconcile()  # does not raise
    out = capsys.readouterr().out
    assert "Warning" not in out and "HTTP" not in out
    assert h.db.get_file("a.txt") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("status", [500, 503])
async def test_reconcile_upload_server_error_is_logged_not_raised(kind, status, tmp_path, capsys):
    h = Harness(kind, tmp_path)
    h.local().write_text("new")
    h.api.upload_file.side_effect = http_error(status)
    await h.engine.reconcile()  # does not raise
    out = capsys.readouterr().out
    assert "Warning" in out and f"HTTP {status}" in out and "a.txt" in out
    assert h.db.get_file("a.txt") is None


@pytest.mark.asyncio
async def test_initial_sync_upload_409_is_silent_and_not_a_failure(tmp_path, capsys):
    h = Harness("dir", tmp_path)
    h.local().write_text("new")
    h.api.upload_file.side_effect = http_error(409)
    summary = await h.engine.initial_sync()
    out = capsys.readouterr().out
    assert "Warning" not in out and "HTTP" not in out
    assert summary["failed"] == 0 and summary["uploaded"] == 0


@pytest.mark.asyncio
async def test_initial_sync_upload_server_error_is_logged_and_counted(tmp_path, capsys):
    h = Harness("dir", tmp_path)
    h.local().write_text("new")
    h.api.upload_file.side_effect = http_error(503)
    summary = await h.engine.initial_sync()
    out = capsys.readouterr().out
    assert "Warning" in out and "HTTP 503" in out and "a.txt" in out
    assert summary["failed"] == 1 and summary["uploaded"] == 0


@pytest.mark.asyncio
async def test_initial_sync_conflict_local_upload_server_error_is_logged_and_counted(tmp_path, capsys):
    h = Harness("dir", tmp_path)
    h.local().write_text("local")
    h.remote(entry())
    h.api.upload_file.side_effect = http_error(500)
    summary = await h.engine.initial_sync(lambda rel, s_file, l_file: "local")
    assert "HTTP 500" in capsys.readouterr().out
    assert summary["failed"] == 1


@pytest.mark.asyncio
async def test_initial_sync_conflict_local_upload_409_is_silent_and_not_a_failure(tmp_path, capsys):
    h = Harness("dir", tmp_path)
    h.local().write_text("local")
    h.remote(entry())
    h.api.upload_file.side_effect = http_error(409)
    summary = await h.engine.initial_sync(lambda rel, s_file, l_file: "local")
    out = capsys.readouterr().out
    assert "Warning" not in out and "HTTP" not in out
    assert summary["failed"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_download_server_error_is_a_retryable_skip(kind, tmp_path, capsys):
    h = Harness(kind, tmp_path)
    h.remote(entry())
    h.api.download_file.side_effect = http_error(500)
    await h.engine.reconcile()  # does not raise
    assert "Warning" in capsys.readouterr().out
    assert not h.local().exists()


# --- 403 is an ordinary HTTP error ------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_upload_403_is_logged_not_raised(kind, tmp_path, capsys):
    h = Harness(kind, tmp_path)
    h.local().write_text("new")
    h.api.upload_file.side_effect = http_error(403)
    await h.engine.reconcile()  # does not raise
    out = capsys.readouterr().out
    assert "Warning" in out and "HTTP 403" in out and "a.txt" in out
    assert h.db.get_file("a.txt") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_download_403_is_a_retryable_skip_that_names_status_and_path(kind, tmp_path, capsys):
    h = Harness(kind, tmp_path)
    h.remote(entry())
    h.api.download_file.side_effect = http_error(403)
    await h.engine.reconcile()  # does not raise
    out = capsys.readouterr().out
    assert "Warning" in out and "403" in out and "a.txt" in out
    assert not h.local().exists()
    assert h.db.get_last_server_revision() == 0  # retryable: fetched again by the next reconcile


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_local_deletion_403_is_logged_not_raised(kind, tmp_path, capsys):
    h = Harness(kind, tmp_path)
    h.track("a.txt")
    h.api.delete_file.side_effect = http_error(403)
    await h.engine.reconcile()  # does not raise
    out = capsys.readouterr().out
    assert "Warning" in out and "HTTP 403" in out and "a.txt" in out
    assert h.db.get_file("a.txt") is not None  # retried by the next reconcile


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_reconcile_get_state_403_raises_as_a_plain_http_error(kind, tmp_path):
    """The raise is fail-fast for `sync`; the daemon loops catch it. It is not an auth error."""
    h = Harness(kind, tmp_path)
    h.api.get_state.side_effect = http_error(403)
    with pytest.raises(httpx.HTTPStatusError) as exc:
        await h.engine.reconcile()
    assert not is_auth_error(exc.value)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("scenario", ["upload", "download", "conflict-server", "conflict-local", "conflict-both"])
async def test_initial_sync_403_is_a_failed_file_not_an_abort(kind, scenario, tmp_path):
    h = Harness(kind, tmp_path)
    if scenario == "upload":
        h.local().write_text("local only")
    elif scenario == "download":
        h.remote(entry())
    else:
        h.local().write_text("local")
        h.remote(entry())
    h.api.upload_file.side_effect = http_error(403)
    h.api.download_file.side_effect = http_error(403)
    choice = scenario.split("-")[1] if scenario.startswith("conflict-") else "ask"
    summary = await h.engine.initial_sync(lambda rel, s_file, l_file: choice)  # does not raise
    assert summary["failed"] == 1


# --- local deletions --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
async def test_local_deletion_server_error_is_logged_and_the_loop_continues(kind, tmp_path, capsys):
    h = Harness(kind, tmp_path, names=("a.txt", "b.txt"))
    h.track("a.txt")
    h.track("b.txt")

    async def delete_file(path, expected_revision=None):
        if path == "a.txt":
            raise http_error(500)
        return {"revision": 9}

    h.api.delete_file.side_effect = delete_file
    await h.engine.reconcile()  # does not raise
    out = capsys.readouterr().out
    assert "Warning" in out and "HTTP 500" in out and "a.txt" in out
    assert {c.args[0] for c in h.api.delete_file.await_args_list} == {"a.txt", "b.txt"}
    assert h.db.get_file("a.txt") is not None  # retried by the next reconcile
    assert h.db.get_file("b.txt") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("status,row_kept", [(409, True), (404, False)])
async def test_local_deletion_409_and_404_keep_their_meaning(kind, status, row_kept, tmp_path, capsys):
    h = Harness(kind, tmp_path)
    h.track("a.txt")
    h.api.delete_file.side_effect = http_error(status)
    await h.engine.reconcile()
    assert "Warning" not in capsys.readouterr().out
    assert (h.db.get_file("a.txt") is not None) is row_kept


# --- CLI --------------------------------------------------------------------------------------


@pytest.fixture
def cli_env(tmp_path, monkeypatch):
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(tmp_path / "config.toml"))  # init writes a default config here
    cfg = ClientConfig(server=ServerConfig(url="https://sync.example.org", api_key=KEY))
    cfg.directories[0].local_path = tmp_path / "sync"
    monkeypatch.setattr(cli_module, "settings", cfg)

    def initialized(value=True):
        db = LocalDB(tmp_path / "sync" / ".syncme" / "local_state.db")
        db.set_initialized(value)

    return initialized


def fail_with(monkeypatch, name, exc):
    monkeypatch.setattr(SyncEngine, name, AsyncMock(side_effect=exc))


@pytest.mark.parametrize("cmd,method", [("sync", "reconcile"), ("run", "reconcile"), ("init", "initial_sync")])
def test_cli_exits_78_with_a_clear_message_on_auth_failure(cli_env, monkeypatch, cmd, method):
    if cmd != "init":
        cli_env()  # sync and run need an initialized directory; init needs an uninitialized one
    fail_with(monkeypatch, method, http_error(401))
    result = CliRunner().invoke(cli_module.cli, [cmd])
    assert result.exit_code == 78
    assert "rejected the API key (HTTP 401)" in result.output
    assert "rejected" in result.output and "key" in result.output and "new" in result.output
    assert KEY not in result.output
    assert "Traceback" not in result.output


def test_cli_run_exits_78_when_the_event_stream_is_rejected_while_running(cli_env, monkeypatch):
    """The Task 9 path: the daemon is up (watcher and poll loop running) when the key is revoked."""
    cli_env()
    monkeypatch.setattr(SyncEngine, "reconcile", AsyncMock())  # the startup reconcile succeeds

    async def rejected(self):
        raise http_error(401)
        yield  # pragma: no cover  (makes this an async generator)

    monkeypatch.setattr(cli_module.APIClient, "listen_events", rejected)
    result = CliRunner().invoke(cli_module.cli, ["run"])
    assert result.exit_code == 78
    assert "HTTP 401" in result.output and KEY not in result.output


@pytest.mark.parametrize("status", [403, 500])
@pytest.mark.parametrize("cmd,method", [("sync", "reconcile"), ("run", "reconcile"), ("init", "initial_sync")])
def test_cli_non_auth_errors_keep_the_existing_behaviour(cli_env, monkeypatch, cmd, method, status):
    """A 403 (proxy or WAF deny) is not exit code 78: systemd must keep restarting the daemon."""
    if cmd != "init":
        cli_env()
    fail_with(monkeypatch, method, http_error(status))
    result = CliRunner().invoke(cli_module.cli, [cmd])
    assert result.exit_code not in (0, EXIT_AUTH)
    assert isinstance(result.exception, httpx.HTTPStatusError)
    assert "rejected the API key" not in result.output


# --- systemd ----------------------------------------------------------------------------------


def test_systemd_unit_does_not_restart_on_exit_code_78():
    text = UNIT_FILE.read_text()
    service = text.split("[Service]", 1)[1].split("[Install]", 1)[0]
    assert "RestartPreventExitStatus=78" in service.splitlines()
    assert "Restart=on-failure" in service  # other failures are still restarted
