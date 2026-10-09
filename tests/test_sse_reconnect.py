"""The event stream reconnects with backoff instead of ending the SSE task for good.

`api.listen_events` is replaced by a scripted factory: every call consumes the next script entry,
a list of events (dicts), exceptions and Advance steps that the stream yields, raises or applies,
in order. A stream that opened starts with CONNECTED, the synthetic first item that
APIClient.listen_events yields once the HTTP status was checked; a script entry without it is a
connection that failed before the stream opened. When the script is used up the next connection
attempt raises Stop, a BaseException, so it escapes the loop's `except Exception` and ends the
test. asyncio.sleep is replaced by a recorder, so the backoff schedule can be asserted without
waiting, and the engine's clock is a fake that only moves on Advance steps.
"""
import asyncio
import time
from unittest.mock import AsyncMock

import httpx
import pytest

from client.api import APIClient
from client.config import FileConfig, ServerConfig
from client.db import LocalDB
from client.engine import SyncEngine
from client.file_engine import FileGroupEngine

KINDS = ["dir", "file"]
CONNECTED = {"event": "connected"}
EVENT = {"event": "file_updated", "path": "a.txt", "revision": 2}


class Stop(BaseException):
    pass


class Advance:
    """Script step: move the fake clock forward by `seconds`."""

    def __init__(self, seconds):
        self.seconds = seconds


def http_error(status):
    request = httpx.Request("GET", "https://x")
    return httpx.HTTPStatusError(f"HTTP {status}", request=request, response=httpx.Response(status, request=request))


class Loop:
    def __init__(self, kind, tmp_path, monkeypatch):
        api = AsyncMock()
        if kind == "dir":
            root = tmp_path / "sync"
            root.mkdir()
            self.engine = SyncEngine(root, LocalDB(root / ".syncme" / "local_state.db"), api)
            self.label = "sync"
        else:
            conf = FileConfig(local_path=tmp_path / "a.txt", workspace="ws", remote_path="a.txt")
            self.engine = FileGroupEngine("ws", [conf], LocalDB(tmp_path / "ws.db"), api, ServerConfig())
            self.label = "ws"
        self.default_clock = self.engine.sse_clock
        self.now = 0.0
        self.engine.sse_clock = lambda: self.now
        self.trace = []
        self.engine.reconcile = AsyncMock(side_effect=lambda: self.trace.append("reconcile"))
        self.sleeps = []
        self.connects = 0

        async def fake_sleep(delay, *args):
            self.sleeps.append(delay)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    async def run(self, script, expect=Stop):
        """Run _sse_loop; by default until the script is used up. Returns the ExceptionInfo of `expect`."""

        def listen_events():
            index = self.connects
            self.connects += 1
            self.trace.append("connect")
            if index >= len(script):
                raise Stop()
            return stream(script[index])

        async def stream(steps):
            for step in steps:
                if isinstance(step, Advance):
                    self.now += step.seconds
                elif isinstance(step, BaseException):
                    raise step
                else:
                    yield step

        self.engine.api.listen_events = listen_events
        with pytest.raises(expect) as exc:
            await asyncio.wait_for(self.engine._sse_loop(), 5)  # the cap turns a hang into a failure
        return exc


@pytest.fixture(params=KINDS)
def loop(request, tmp_path, monkeypatch):
    return Loop(request.param, tmp_path, monkeypatch)


@pytest.mark.asyncio
async def test_defaults_are_1s_and_60s_and_a_stream_is_healthy_after_30s(loop):
    assert loop.engine.sse_initial_delay == 1.0
    assert loop.engine.sse_max_delay == 60.0
    assert loop.engine.sse_healthy_seconds == 30.0


@pytest.mark.asyncio
async def test_the_default_clock_is_time_monotonic(loop):
    assert loop.default_clock is time.monotonic


@pytest.mark.asyncio
async def test_a_stream_closed_by_the_server_is_reopened_and_reconciled_once_it_is_open(loop):
    await loop.run([[CONNECTED], [CONNECTED]])
    assert loop.connects == 3                      # first stream, reconnect, reconnect attempt that ends the test
    assert loop.engine.reconcile.await_count == 2  # one catch-up per opened stream
    assert loop.sleeps == [1.0, 2.0]


@pytest.mark.asyncio
async def test_the_catch_up_reconcile_runs_after_the_stream_is_open_not_before(loop):
    """A reconcile before the reconnect would lose the events that happen in the gap."""
    await loop.run([[httpx.ConnectError("x")], [CONNECTED]])
    assert loop.trace == ["connect", "connect", "reconcile", "connect"]


@pytest.mark.asyncio
async def test_no_reconcile_runs_while_the_stream_cannot_be_opened(loop):
    await loop.run([[http_error(503)]] * 3)
    assert loop.engine.reconcile.await_count == 0
    assert loop.sleeps == [1.0, 2.0, 4.0]


@pytest.mark.asyncio
async def test_the_connected_event_and_every_event_trigger_a_reconcile(loop):
    await loop.run([[CONNECTED, EVENT, EVENT]])
    assert loop.engine.reconcile.await_count == 3  # connected, plus two events; nothing extra after the stream ended


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [httpx.ConnectError("refused"), httpx.ReadTimeout("silent"), http_error(503)])
async def test_network_and_server_errors_reconnect_with_backoff(loop, exc, capsys):
    await loop.run([[exc], [exc], [CONNECTED]])
    assert loop.connects == 4
    assert loop.sleeps == [1.0, 2.0, 4.0]
    assert loop.engine.reconcile.await_count == 1
    out = capsys.readouterr().out
    assert f"[{loop.label}]" in out and type(exc).__name__ in out and "reconnecting in 1s" in out


@pytest.mark.asyncio
async def test_backoff_doubles_up_to_the_cap(loop):
    await loop.run([[httpx.ConnectError("x")]] * 8)
    assert loop.sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]


@pytest.mark.asyncio
async def test_backoff_uses_the_instance_attributes(loop):
    loop.engine.sse_initial_delay = 0.5
    loop.engine.sse_max_delay = 3.0
    await loop.run([[httpx.ConnectError("x")]] * 5)
    assert loop.sleeps == [0.5, 1.0, 2.0, 3.0, 3.0]


# --- the delay resets only after a stream that stayed open -------------------------------------


@pytest.mark.asyncio
async def test_a_server_that_closes_the_stream_after_every_event_does_not_cause_a_one_second_loop(loop):
    await loop.run([[CONNECTED, EVENT]] * 5)
    assert loop.sleeps == [1.0, 2.0, 4.0, 8.0, 16.0]


@pytest.mark.asyncio
async def test_backoff_resets_after_a_stream_that_stayed_open_for_30_seconds(loop):
    boom = httpx.ConnectError("x")
    await loop.run([[boom], [boom], [CONNECTED, EVENT, Advance(30.0), boom], [boom]])
    assert loop.sleeps == [1.0, 2.0, 1.0, 2.0]


@pytest.mark.asyncio
async def test_stream_uptime_is_measured_from_the_connection_not_from_the_last_event(loop):
    boom = httpx.ConnectError("x")
    await loop.run([[boom], [boom], [CONNECTED, Advance(20.0), EVENT, Advance(20.0), boom], [boom]])
    assert loop.sleeps == [1.0, 2.0, 1.0, 2.0]  # open for 40 s in total, only 20 s since the last event


@pytest.mark.asyncio
async def test_backoff_does_not_reset_when_the_stream_was_open_for_slightly_less_than_30_seconds(loop):
    boom = httpx.ConnectError("x")
    await loop.run([[boom], [boom], [CONNECTED, Advance(29.9), boom], [boom]])
    assert loop.sleeps == [1.0, 2.0, 4.0, 8.0]


@pytest.mark.asyncio
async def test_backoff_also_resets_after_a_healthy_stream_that_the_server_closed_cleanly(loop):
    boom = httpx.ConnectError("x")
    await loop.run([[boom], [boom], [CONNECTED, Advance(31.0)], []])
    assert loop.sleeps == [1.0, 2.0, 1.0, 2.0]


@pytest.mark.asyncio
async def test_the_log_line_of_a_healthy_stream_names_the_reset_delay(loop, capsys):
    boom = httpx.ConnectError("x")
    await loop.run([[boom], [boom], [CONNECTED, Advance(30.0), boom], [CONNECTED]])
    out = capsys.readouterr().out
    assert out.count("reconnecting in 1s") == 2 and out.count("reconnecting in 2s") == 1
    assert "reconnecting in 4s" not in out


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds,reset", [(4.9, False), (5.0, True)])
async def test_the_healthy_threshold_is_the_instance_attribute(loop, seconds, reset):
    loop.engine.sse_healthy_seconds = 5.0
    boom = httpx.ConnectError("x")
    await loop.run([[boom], [boom], [CONNECTED, Advance(seconds), boom], [boom]])
    assert loop.sleeps == ([1.0, 2.0, 1.0, 2.0] if reset else [1.0, 2.0, 4.0, 8.0])


@pytest.mark.asyncio
async def test_time_spent_in_a_failed_connection_attempt_is_not_stream_uptime(loop):
    """Only a stream that opened (CONNECTED) can be healthy; the clock moving during a failed attempt does not count."""
    await loop.run([[Advance(100.0), httpx.ConnectError("x")]] * 3)
    assert loop.sleeps == [1.0, 2.0, 4.0]


# --- errors ------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_auth_error_from_the_stream_ends_the_loop_without_retrying(loop):
    exc = await loop.run([[http_error(401)]], expect=httpx.HTTPStatusError)
    assert exc.value.response.status_code == 401
    assert loop.connects == 1 and loop.sleeps == []
    assert loop.engine.reconcile.await_count == 0


@pytest.mark.asyncio
async def test_a_403_from_the_stream_is_a_proxy_deny_and_reconnects_with_backoff(loop, capsys):
    """HAProxy `http-request deny` answers 403: never stop for good, log the status and keep trying."""
    await loop.run([[http_error(403)], [http_error(403)], []])
    assert loop.connects == 4
    assert loop.sleeps == [1.0, 2.0, 4.0]
    out = capsys.readouterr().out
    assert f"[{loop.label}] Event stream interrupted" in out and "HTTP 403" in out


@pytest.mark.asyncio
async def test_auth_error_from_the_reconcile_on_connect_propagates(loop):
    loop.engine.reconcile.side_effect = http_error(401)
    await loop.run([[CONNECTED]], expect=httpx.HTTPStatusError)
    assert loop.sleeps == []


@pytest.mark.asyncio
async def test_other_reconcile_failures_are_printed_and_the_loop_continues(loop, capsys):
    loop.engine.reconcile.side_effect = [RuntimeError("disk full"), None, None]
    await loop.run([[CONNECTED, EVENT], [CONNECTED]])
    out = capsys.readouterr().out
    assert f"[{loop.label}] Reconcile failed: RuntimeError: disk full" in out
    assert loop.connects == 3                      # the SSE task survived the failing reconcile
    assert loop.engine.reconcile.await_count == 3


@pytest.mark.asyncio
async def test_cancellation_from_the_stream_propagates_immediately(loop):
    await loop.run([[asyncio.CancelledError()]], expect=asyncio.CancelledError)
    assert loop.connects == 1 and loop.sleeps == []


@pytest.mark.asyncio
async def test_cancellation_inside_reconcile_propagates(loop):
    loop.engine.reconcile.side_effect = asyncio.CancelledError()
    await loop.run([[CONNECTED]], expect=asyncio.CancelledError)
    assert loop.sleeps == []


# --- APIClient.listen_events ------------------------------------------------------------------


async def collect(handler):
    api = APIClient("https://sync.example.org", "k" * 43, "laptop", workspace_id="ws1", timeout=300.0)
    await api.client.aclose()
    api.client = httpx.AsyncClient(transport=httpx.MockTransport(handler), headers=api.headers)
    events = []
    try:
        async for e in api.listen_events():
            events.append(e)
    finally:
        await api.close()
    return events


@pytest.mark.asyncio
async def test_listen_events_timeouts_are_15s_connect_and_60s_read():
    seen = {}

    def handler(request):
        seen.update(request.extensions["timeout"])
        return httpx.Response(200, content=b'data: {"event": "file_updated"}\n\n: ping\n\n')

    events = await collect(handler)
    assert events == [CONNECTED, {"event": "file_updated"}]
    assert seen["read"] == 60.0 and seen["connect"] == 15.0


@pytest.mark.asyncio
async def test_listen_events_yields_a_synthetic_connected_item_first():
    body = b'data: {"event": "file_updated", "path": "a"}\n\ndata: {"event": "file_deleted", "path": "b"}\n\n'
    events = await collect(lambda request: httpx.Response(200, content=body))
    assert events == [CONNECTED, {"event": "file_updated", "path": "a"}, {"event": "file_deleted", "path": "b"}]


@pytest.mark.asyncio
async def test_listen_events_on_an_empty_stream_yields_only_connected():
    assert await collect(lambda request: httpx.Response(200, content=b"")) == [CONNECTED]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 403, 503])
async def test_listen_events_yields_nothing_when_the_status_is_an_error(status):
    """connected means the stream is open: an error status must raise before anything is yielded."""
    events = []
    api = APIClient("https://sync.example.org", "k" * 43, "laptop", workspace_id="ws1", timeout=300.0)
    await api.client.aclose()
    api.client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(status, content=b"nope")), headers=api.headers)
    try:
        with pytest.raises(httpx.HTTPStatusError):
            async for e in api.listen_events():
                events.append(e)
    finally:
        await api.close()
    assert events == []
