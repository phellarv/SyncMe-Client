"""The poll and watcher loops survive a failing reconcile, like the event stream loop does.

Only a rejected key (HTTP 401) ends them. The very first reconcile in run() stays fail-fast:
if it fails the process exits non-zero and systemd restarts it.
"""
import asyncio
from unittest.mock import AsyncMock

import httpx
import pytest

import client.file_engine as file_engine_module
from client.config import FileConfig, ServerConfig
from client.db import LocalDB
from client.engine import SyncEngine
from client.file_engine import FileGroupEngine

KINDS = ["dir", "file"]


class Stop(BaseException):
    pass


def http_error(status):
    request = httpx.Request("GET", "https://x")
    return httpx.HTTPStatusError(f"HTTP {status}", request=request, response=httpx.Response(status, request=request))


class FakeWatcher:
    """Stands in for client.watcher.Watcher: yields scripted batches, records start and stop."""

    batches = []

    def __init__(self, *args, **kwargs):
        self.started = False
        self.stopped = False

    def start(self, loop=None):
        self.started = True

    def stop(self):
        self.stopped = True

    async def get_batches(self):
        for batch in type(self).batches:
            yield batch


class Rig:
    def __init__(self, kind, tmp_path, monkeypatch):
        api = AsyncMock()
        self.kind = kind
        if kind == "dir":
            root = tmp_path / "sync"
            root.mkdir()
            self.engine = SyncEngine(root, LocalDB(root / ".syncme" / "local_state.db"), api, poll_interval_seconds=1)
            self.label = "sync"
            self.engine.watcher = FakeWatcher()
            self.watcher = self.engine.watcher
            self.batch = [("a.txt", "modified")]
        else:
            conf = FileConfig(local_path=tmp_path / "a.txt", workspace="ws", remote_path="a.txt")
            self.engine = FileGroupEngine("ws", [conf], LocalDB(tmp_path / "ws.db"), api, ServerConfig())
            self.label = "ws"
            created = []

            class Recording(FakeWatcher):
                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    created.append(self)

            monkeypatch.setattr(file_engine_module, "Watcher", Recording)
            self.created = created
            self.batch = [("a.txt", "modified")]
        self.engine.reconcile = AsyncMock()
        self.sleeps = []

        async def fake_sleep(delay, *args):
            self.sleeps.append(delay)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        FakeWatcher.batches = []

    @property
    def stopped(self):
        return self.watcher.stopped if self.kind == "dir" else all(w.stopped for w in self.created) and bool(self.created)

    async def poll(self, expect):
        with pytest.raises(expect) as exc:
            await asyncio.wait_for(self.engine._poll_loop(), 5)  # the cap turns a hang into a failure
        return exc

    async def watch(self, batches, expect=None):
        FakeWatcher.batches = batches
        if expect is None:
            await asyncio.wait_for(self.engine._watch_loop(), 5)
            return None
        with pytest.raises(expect) as exc:
            await asyncio.wait_for(self.engine._watch_loop(), 5)
        return exc


@pytest.fixture(params=KINDS)
def rig(request, tmp_path, monkeypatch):
    return Rig(request.param, tmp_path, monkeypatch)


# --- poll loop ----------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 503])
async def test_poll_loop_logs_a_failing_reconcile_and_keeps_running(rig, status, capsys):
    rig.engine.reconcile.side_effect = [http_error(status), None, Stop()]
    if rig.kind == "file":
        rig.engine.server_config.poll_interval_seconds = 1
    await rig.poll(Stop)
    assert rig.engine.reconcile.await_count == 3  # the failure did not end the loop
    out = capsys.readouterr().out
    assert f"[{rig.label}] Reconcile failed: HTTPStatusError" in out


@pytest.mark.asyncio
async def test_poll_loop_survives_a_403_proxy_deny(rig, capsys):
    rig.engine.reconcile.side_effect = [http_error(403), None, Stop()]
    if rig.kind == "file":
        rig.engine.server_config.poll_interval_seconds = 1
    await rig.poll(Stop)
    assert rig.engine.reconcile.await_count == 3
    assert "Reconcile failed" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_poll_loop_propagates_a_rejected_key(rig):
    rig.engine.reconcile.side_effect = http_error(401)
    if rig.kind == "file":
        rig.engine.server_config.poll_interval_seconds = 1
    exc = await rig.poll(httpx.HTTPStatusError)
    assert exc.value.response.status_code == 401
    assert rig.engine.reconcile.await_count == 1


@pytest.mark.asyncio
async def test_poll_loop_does_not_swallow_cancellation(rig):
    rig.engine.reconcile.side_effect = asyncio.CancelledError()
    if rig.kind == "file":
        rig.engine.server_config.poll_interval_seconds = 1
    await rig.poll(asyncio.CancelledError)


# --- watcher loop -------------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 503])
async def test_watch_loop_logs_a_failing_reconcile_and_keeps_running(rig, status, capsys):
    rig.engine.reconcile.side_effect = [http_error(status), None, None]
    await rig.watch([rig.batch, rig.batch, rig.batch])
    assert rig.engine.reconcile.await_count == 3  # all three batches were handled
    assert f"[{rig.label}] Reconcile failed: HTTPStatusError" in capsys.readouterr().out
    assert rig.stopped


@pytest.mark.asyncio
async def test_watch_loop_survives_a_403_proxy_deny(rig, capsys):
    rig.engine.reconcile.side_effect = [http_error(403), None]
    await rig.watch([rig.batch, rig.batch])
    assert rig.engine.reconcile.await_count == 2
    assert "Reconcile failed" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_watch_loop_propagates_a_rejected_key_and_stops_the_watcher(rig):
    rig.engine.reconcile.side_effect = http_error(401)
    exc = await rig.watch([rig.batch, rig.batch], expect=httpx.HTTPStatusError)
    assert exc.value.response.status_code == 401
    assert rig.engine.reconcile.await_count == 1
    assert rig.stopped


@pytest.mark.asyncio
async def test_watch_loop_does_not_swallow_cancellation(rig):
    rig.engine.reconcile.side_effect = asyncio.CancelledError()
    await rig.watch([rig.batch], expect=asyncio.CancelledError)


# --- run(): the first reconcile stays fail-fast -------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 503])
async def test_run_does_not_start_the_loops_when_the_first_reconcile_fails(rig, status):
    rig.engine.reconcile.side_effect = http_error(status)
    rig.engine._watch_loop = AsyncMock()
    rig.engine._sse_loop = AsyncMock()
    rig.engine._poll_loop = AsyncMock()
    with pytest.raises(httpx.HTTPStatusError):
        await rig.engine.run()
    assert rig.engine.reconcile.await_count == 1
    rig.engine._watch_loop.assert_not_awaited()
    rig.engine._sse_loop.assert_not_awaited()
    rig.engine._poll_loop.assert_not_awaited()


@pytest.mark.asyncio
async def test_run_starts_all_three_loops_after_a_successful_first_reconcile(rig):
    rig.engine._watch_loop = AsyncMock()
    rig.engine._sse_loop = AsyncMock()
    rig.engine._poll_loop = AsyncMock()
    await rig.engine.run()
    assert rig.engine.reconcile.await_count == 1
    rig.engine._watch_loop.assert_awaited_once()
    rig.engine._sse_loop.assert_awaited_once()
    rig.engine._poll_loop.assert_awaited_once()
