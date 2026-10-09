"""Data-safety tests for the download paths of FileGroupEngine (parity with SyncEngine).

* Nothing local may change before the downloaded content has been fetched and verified against
  its sha256 (conflict rename, symlink unlink, archive). A hash mismatch or a network error must
  leave the local file exactly as it was.
* A failed fetch is retryable: the stored server revision is not advanced (reconcile) and the
  file is counted as failed (initial_sync), exactly like SyncEngine does.

The paths come from the user's config (FileConfig.local_path), so there is no safe_join here.
"""
import asyncio
import hashlib
import os
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from client.config import FileConfig, ServerConfig
from client.db import LocalDB
from client.file_engine import FileGroupEngine

PAYLOAD = b"server payload"
GOOD_SHA = hashlib.sha256(PAYLOAD).hexdigest()
MODES = ["mismatch", "network", "http500"]


@pytest.fixture
def make_engine(tmp_path):
    def _make(names=("doc.txt",), replace_symlinks=False, initialized=True):
        root = tmp_path / "files"
        root.mkdir(exist_ok=True)
        confs = [FileConfig(local_path=root / n, workspace="ws", remote_path=n, replace_symlinks=replace_symlinks) for n in names]
        db = LocalDB(tmp_path / "state" / "ws.db")
        db.set_initialized(initialized)
        api = AsyncMock()
        api.download_file.return_value = PAYLOAD
        api.upload_file.return_value = {"revision": 9}
        engine = FileGroupEngine("ws", confs, db, api, ServerConfig())
        engine.root = root
        return engine

    return _make


def _entry(path, **overrides):
    entry = {"path": path, "revision": 2, "is_deleted": False, "sha256": GOOD_SHA}
    entry.update(overrides)
    return entry


def _state(*files):
    return {"files": list(files), "latest_revision": 5}


def _break_download(engine, mode):
    if mode == "mismatch":
        engine.api.download_file.return_value = b"tampered content"
    elif mode == "network":
        engine.api.download_file.side_effect = httpx.ConnectError("boom")
    else:
        request = httpx.Request("GET", "https://x")
        engine.api.download_file.side_effect = httpx.HTTPStatusError("HTTP 500", request=request, response=httpx.Response(500, request=request))


def _edited_file(engine, name="doc.txt", text="local edit"):
    """A synced file that was edited locally since the last sync."""
    f = engine.root / name
    f.write_text(text)
    engine.db.upsert_file(name, "old_hash", 10, f.stat().st_mtime - 100, 1, False)
    engine.api.upload_file.side_effect = httpx.ConnectError("offline")  # keep the upload step from rewriting the row
    return f


def _archive_is_empty(engine):
    archive = engine.root / ".syncme" / "archive"
    return not archive.exists() or not any(archive.iterdir())


def _conflicts(engine):
    return list(engine.root.glob("*.conflict.*"))


def _link(engine, target_text="hello"):
    target = engine.root / "target.txt"
    target.write_text(target_text)
    link = engine.root / "link.txt"
    link.symlink_to("target.txt")
    return link, target


# --------------------------------------------------------------------------
# reconcile
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
async def test_reconcile_failed_fetch_keeps_locally_edited_file(make_engine, mode, capsys):
    engine = make_engine()
    f = _edited_file(engine)
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    _break_download(engine, mode)

    with patch("client.file_engine.handle_conflict_notification") as notify:
        await engine.reconcile()

    assert f.read_text() == "local edit"
    assert _conflicts(engine) == []
    notify.assert_not_called()
    assert engine.db.get_file("doc.txt")["server_revision"] == 1
    out = capsys.readouterr().out
    assert "Warning" in out
    if mode == "mismatch":
        assert "hash mismatch for doc.txt" in out


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
async def test_reconcile_failed_fetch_does_not_advance_the_stored_revision(make_engine, mode, capsys):
    engine = make_engine()
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    _break_download(engine, mode)

    await engine.reconcile()

    assert engine.db.get_last_server_revision() == 0
    assert not (engine.root / "doc.txt").exists()
    assert "will be retried on the next reconcile; server revision not advanced" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_reconcile_retries_a_failed_fetch_on_the_next_reconcile(make_engine):
    engine = make_engine()
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    engine.api.download_file.return_value = b"tampered content"
    await engine.reconcile()
    assert not (engine.root / "doc.txt").exists()

    engine.api.download_file.return_value = PAYLOAD
    await engine.reconcile()

    assert (engine.root / "doc.txt").read_bytes() == PAYLOAD
    assert engine.db.get_last_server_revision() == 5


@pytest.mark.asyncio
async def test_reconcile_verified_fetch_is_written_and_the_revision_advances(make_engine):
    engine = make_engine()
    engine.api.get_state.return_value = _state(_entry("doc.txt"))

    await engine.reconcile()

    assert (engine.root / "doc.txt").read_bytes() == PAYLOAD
    assert engine.db.get_file("doc.txt")["sha256"] == GOOD_SHA
    assert engine.db.get_last_server_revision() == 5


@pytest.mark.asyncio
async def test_reconcile_one_bad_fetch_does_not_stop_the_other_files(make_engine):
    engine = make_engine(names=("a.txt", "b.txt"))
    engine.api.get_state.return_value = _state(_entry("a.txt"), _entry("b.txt"))

    async def download(path):
        return b"tampered content" if path == "a.txt" else PAYLOAD

    engine.api.download_file.side_effect = download

    await engine.reconcile()

    assert not (engine.root / "a.txt").exists()
    assert (engine.root / "b.txt").read_bytes() == PAYLOAD
    assert engine.db.get_last_server_revision() == 0  # a.txt must be fetched again


@pytest.mark.asyncio
@pytest.mark.parametrize("sha", [None, ""])
async def test_reconcile_missing_or_empty_sha256_skips_verification(make_engine, sha):
    engine = make_engine()
    entry = _entry("doc.txt")
    if sha is None:
        del entry["sha256"]
    else:
        entry["sha256"] = sha
    engine.api.get_state.return_value = _state(entry)

    await engine.reconcile()

    assert (engine.root / "doc.txt").read_bytes() == PAYLOAD
    assert engine.db.get_last_server_revision() == 5


@pytest.mark.asyncio
async def test_reconcile_auth_error_from_the_fetch_still_propagates(make_engine):
    engine = make_engine()
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    request = httpx.Request("GET", "https://x")
    engine.api.download_file.side_effect = httpx.HTTPStatusError("HTTP 401", request=request, response=httpx.Response(401, request=request))

    with pytest.raises(httpx.HTTPStatusError):
        await engine.reconcile()

    assert engine.db.get_last_server_revision() == 0


@pytest.mark.asyncio
async def test_reconcile_conflict_fetches_before_touching_local_file(make_engine):
    engine = make_engine()
    f = _edited_file(engine)
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    seen = {}

    async def download(path):
        seen["local"] = f.read_text()
        seen["conflicts"] = _conflicts(engine)
        return PAYLOAD

    engine.api.download_file.side_effect = download
    at_notify = {}

    async def fake_notify(workspace, rel_path, server_file, conflict_file, on_resolve=None, runner=None):
        at_notify["server"] = server_file.read_bytes()
        at_notify["conflict"] = conflict_file.read_text()

    with patch("client.file_engine.handle_conflict_notification", side_effect=fake_notify):
        await engine.reconcile()
        await asyncio.gather(*engine._tasks)

    assert seen == {"local": "local edit", "conflicts": []}
    assert f.read_bytes() == PAYLOAD
    conflicts = list(engine.root.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1 and conflicts[0].read_text() == "local edit"
    assert at_notify == {"server": PAYLOAD, "conflict": "local edit"}


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
async def test_reconcile_replace_symlinks_failed_fetch_keeps_symlink(make_engine, mode):
    engine = make_engine(names=("link.txt",), replace_symlinks=True)
    link, target = _link(engine)
    engine.api.get_state.return_value = _state(_entry("link.txt"))
    _break_download(engine, mode)

    await engine.reconcile()

    assert link.is_symlink() and os.readlink(link) == "target.txt"
    assert target.read_text() == "hello"


@pytest.mark.asyncio
async def test_reconcile_replace_symlinks_verified_fetch_replaces_the_link_only(make_engine):
    engine = make_engine(names=("link.txt",), replace_symlinks=True)
    link, target = _link(engine)
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    await engine.reconcile()

    assert not link.is_symlink() and link.read_bytes() == PAYLOAD
    assert target.read_text() == "hello"


@pytest.mark.asyncio
async def test_reconcile_symlink_without_replace_symlinks_is_left_alone_and_not_a_retry(make_engine):
    engine = make_engine(names=("link.txt",), replace_symlinks=False)
    link, target = _link(engine)
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    await engine.reconcile()

    assert link.is_symlink() and target.read_text() == "hello"
    engine.api.download_file.assert_not_awaited()
    assert engine.db.get_last_server_revision() == 5  # a deliberate skip never holds the revision back


# --------------------------------------------------------------------------
# initial_sync: remote only
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
async def test_initial_sync_remote_only_failed_fetch_writes_nothing_and_counts_as_failed(make_engine, mode):
    engine = make_engine(initialized=False)
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    _break_download(engine, mode)

    summary = await engine.initial_sync()

    assert not (engine.root / "doc.txt").exists()
    assert summary["failed"] == 1 and summary["downloaded"] == 0
    assert engine.db.get_file("doc.txt") is None
    assert engine.db.is_initialized()
    assert engine.db.get_last_server_revision() == 0  # the next reconcile fetches it again


@pytest.mark.asyncio
async def test_initial_sync_verified_download_advances_the_revision(make_engine):
    engine = make_engine(initialized=False)
    engine.api.get_state.return_value = _state(_entry("doc.txt"))

    summary = await engine.initial_sync()

    assert summary["failed"] == 0 and summary["downloaded"] == 1
    assert (engine.root / "doc.txt").read_bytes() == PAYLOAD
    assert engine.db.get_last_server_revision() == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
async def test_initial_sync_remote_only_replace_symlinks_failed_fetch_keeps_symlink(make_engine, mode):
    engine = make_engine(names=("link.txt",), replace_symlinks=True, initialized=False)
    link = engine.root / "link.txt"
    link.symlink_to("missing.txt")  # broken link: not a local file, so remote-only
    engine.api.get_state.return_value = _state(_entry("link.txt"))
    _break_download(engine, mode)

    summary = await engine.initial_sync()

    assert link.is_symlink() and os.readlink(link) == "missing.txt"
    assert summary["failed"] == 1 and summary["downloaded"] == 0


@pytest.mark.asyncio
async def test_initial_sync_remote_only_replace_symlinks_verified_fetch_replaces_link(make_engine):
    engine = make_engine(names=("link.txt",), replace_symlinks=True, initialized=False)
    link = engine.root / "link.txt"
    link.symlink_to("missing.txt")
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    summary = await engine.initial_sync()

    assert not link.is_symlink() and link.read_bytes() == PAYLOAD
    assert summary["downloaded"] == 1


# --------------------------------------------------------------------------
# initial_sync: conflicts
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("choice", ["server", "both"])
async def test_initial_sync_conflict_failed_fetch_keeps_local_file_and_counts_as_failed(make_engine, choice, mode):
    engine = make_engine(initialized=False)
    f = engine.root / "doc.txt"
    f.write_text("local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    _break_download(engine, mode)

    summary = await engine.initial_sync(lambda *a: choice)

    assert f.read_text() == "local edit"
    assert _conflicts(engine) == []
    assert _archive_is_empty(engine)
    assert summary["conflicts"] == 1 and summary["failed"] == 1
    assert engine.db.get_file("doc.txt") is None
    assert engine.db.get_last_server_revision() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["server", "both"])
async def test_initial_sync_conflict_fetches_before_touching_local_file(make_engine, choice):
    engine = make_engine(initialized=False)
    f = engine.root / "doc.txt"
    f.write_text("local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    seen = {}

    async def download(path):
        seen["local"] = f.read_text()
        seen["conflicts"] = _conflicts(engine)
        seen["archive_empty"] = _archive_is_empty(engine)
        return PAYLOAD

    engine.api.download_file.side_effect = download

    summary = await engine.initial_sync(lambda *a: choice)

    assert seen == {"local": "local edit", "conflicts": [], "archive_empty": True}
    assert summary["failed"] == 0
    assert f.read_bytes() == PAYLOAD
    if choice == "both":
        conflicts = list(engine.root.glob("doc.conflict.*.txt"))
        assert len(conflicts) == 1 and conflicts[0].read_text() == "local edit"
    else:
        archived = list((engine.root / ".syncme" / "archive").iterdir())
        assert len(archived) == 1 and archived[0].read_text() == "local edit"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("choice", ["server", "both"])
async def test_initial_sync_conflict_replace_symlinks_failed_fetch_keeps_symlink(make_engine, choice, mode):
    engine = make_engine(names=("link.txt",), replace_symlinks=True, initialized=False)
    link, target = _link(engine)
    engine.api.get_state.return_value = _state(_entry("link.txt"))
    _break_download(engine, mode)

    await engine.initial_sync(lambda *a: choice)

    assert link.is_symlink() and os.readlink(link) == "target.txt"
    assert target.read_text() == "hello"
    assert _conflicts(engine) == []
    assert _archive_is_empty(engine)


@pytest.mark.asyncio
async def test_initial_sync_server_choice_replace_symlinks_verified_fetch_replaces_link(make_engine):
    engine = make_engine(names=("link.txt",), replace_symlinks=True, initialized=False)
    link, target = _link(engine)
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    await engine.initial_sync(lambda *a: "server")

    assert not link.is_symlink() and link.read_bytes() == PAYLOAD
    assert target.read_text() == "hello"


# --------------------------------------------------------------------------
# initial_sync: every conflict-resolution branch counts its failures
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("choice", ["server", "both"])
async def test_initial_sync_conflict_download_branches_count_each_failure_kind(make_engine, choice, mode):
    engine = make_engine(initialized=False)
    (engine.root / "doc.txt").write_text("local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    _break_download(engine, mode)  # mismatch is the "content is None" site, the others the except site

    summary = await engine.initial_sync(lambda *a: choice)

    assert summary["failed"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 503])
async def test_initial_sync_conflict_local_upload_error_counts_as_failed(make_engine, status):
    engine = make_engine(initialized=False)
    (engine.root / "doc.txt").write_text("local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    request = httpx.Request("PUT", "https://x")
    engine.api.upload_file.side_effect = httpx.HTTPStatusError(f"HTTP {status}", request=request, response=httpx.Response(status, request=request))

    summary = await engine.initial_sync(lambda *a: "local")

    assert summary["conflicts"] == 1 and summary["failed"] == 1
    assert engine.db.get_file("doc.txt") is None
    assert engine.db.get_last_server_revision() == 0


@pytest.mark.asyncio
async def test_initial_sync_conflict_local_upload_network_error_counts_as_failed(make_engine):
    engine = make_engine(initialized=False)
    (engine.root / "doc.txt").write_text("local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    engine.api.upload_file.side_effect = httpx.ConnectError("offline")

    summary = await engine.initial_sync(lambda *a: "local")

    assert summary["failed"] == 1


# --------------------------------------------------------------------------
# replay safety: a held-back revision makes reconcile see the same entries again,
# so the guards SyncEngine has for untracked files and for deletions are needed here too
# --------------------------------------------------------------------------


def _all_texts(engine):
    return {p.read_bytes() for p in engine.root.iterdir() if p.is_file()}


@pytest.mark.asyncio
@pytest.mark.parametrize("choice,failure", [("local", "upload503"), ("server", "mismatch"), ("both", "mismatch"), ("server", "network")])
async def test_reconcile_after_a_failed_initial_sync_conflict_never_destroys_the_users_file(make_engine, choice, failure):
    engine = make_engine(initialized=False)
    f = engine.root / "doc.txt"
    f.write_text("local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    if failure == "upload503":
        request = httpx.Request("PUT", "https://x")
        engine.api.upload_file.side_effect = httpx.HTTPStatusError("HTTP 503", request=request, response=httpx.Response(503, request=request))
    else:
        _break_download(engine, failure)
    summary = await engine.initial_sync(lambda *a: choice)
    assert summary["failed"] == 1 and engine.db.get_file("doc.txt") is None  # no row, revision held back

    # The server is fine again; the same entry comes back with the file present and untracked.
    engine.api.download_file.side_effect = None
    engine.api.download_file.return_value = PAYLOAD
    engine.api.upload_file.side_effect = httpx.ConnectError("offline")
    with patch("client.file_engine.handle_conflict_notification") as notify:
        await engine.reconcile()
        await asyncio.gather(*engine._tasks)

    assert b"local edit" in _all_texts(engine)  # nothing was overwritten without a copy
    conflicts = list(engine.root.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1 and conflicts[0].read_text() == "local edit"
    assert f.read_bytes() == PAYLOAD
    notify.assert_called_once()


@pytest.mark.asyncio
async def test_reconcile_adopts_an_untracked_file_that_already_matches_the_server(make_engine):
    engine = make_engine()
    f = engine.root / "doc.txt"
    f.write_bytes(PAYLOAD)
    engine.api.get_state.return_value = _state(_entry("doc.txt"))

    await engine.reconcile()

    engine.api.download_file.assert_not_awaited()
    assert f.read_bytes() == PAYLOAD and _conflicts(engine) == []
    row = engine.db.get_file("doc.txt")
    assert row["sha256"] == GOOD_SHA and row["server_revision"] == 2


@pytest.mark.asyncio
async def test_a_replayed_deletion_never_eats_a_file_the_user_created_since(make_engine):
    engine = make_engine(names=("a.txt", "b.txt"))
    a = engine.root / "a.txt"
    a.write_text("original")
    sha = hashlib.sha256(b"original").hexdigest()
    engine.db.upsert_file("a.txt", sha, 8, a.stat().st_mtime, 1, False)
    engine.api.get_state.return_value = _state(_entry("a.txt", is_deleted=True, revision=3), _entry("b.txt", revision=4))

    async def download(path):
        return b"tampered content"  # b.txt never verifies, so the revision stays held back

    engine.api.download_file.side_effect = download

    await engine.reconcile()
    assert not a.exists() and engine.db.get_last_server_revision() == 0  # deleted (and archived) once

    a.write_text("brand new")
    await engine.reconcile()  # the deletion is replayed

    assert a.read_text() == "brand new"


@pytest.mark.asyncio
async def test_a_stale_deletion_is_skipped(make_engine):
    engine = make_engine()
    a = engine.root / "doc.txt"
    a.write_text("kept")
    engine.db.upsert_file("doc.txt", hashlib.sha256(b"kept").hexdigest(), 4, a.stat().st_mtime, 5, False)
    engine.api.get_state.return_value = _state(_entry("doc.txt", is_deleted=True, revision=3))

    await engine.reconcile()

    assert a.read_text() == "kept" and engine.db.get_file("doc.txt")["server_revision"] == 5
    assert _archive_is_empty(engine)


@pytest.mark.asyncio
async def test_a_deletion_of_a_file_that_is_already_gone_drops_the_row_without_a_server_delete(make_engine):
    engine = make_engine()
    engine.db.upsert_file("doc.txt", "h" * 64, 1, 1.0, 1, False)
    engine.api.get_state.return_value = _state(_entry("doc.txt", is_deleted=True, revision=3))

    await engine.reconcile()

    assert engine.db.get_file("doc.txt") is None
    engine.api.delete_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_deletion_of_an_unchanged_tracked_file_archives_and_removes_it(make_engine):
    engine = make_engine()
    a = engine.root / "doc.txt"
    a.write_text("old")
    engine.db.upsert_file("doc.txt", hashlib.sha256(b"old").hexdigest(), 3, a.stat().st_mtime, 1, False)
    engine.api.get_state.return_value = _state(_entry("doc.txt", is_deleted=True, revision=3))

    await engine.reconcile()

    assert not a.exists() and engine.db.get_file("doc.txt") is None
    engine.api.delete_file.assert_not_awaited()  # the row went with the file: no server DELETE for it afterwards
    archived = list((engine.root / ".syncme" / "archive").iterdir())
    assert len(archived) == 1 and archived[0].read_text() == "old"


@pytest.mark.asyncio
async def test_a_deletion_at_exactly_the_local_revision_is_skipped(make_engine):
    engine = make_engine()
    a = engine.root / "doc.txt"
    a.write_text("kept")
    engine.db.upsert_file("doc.txt", hashlib.sha256(b"kept").hexdigest(), 4, a.stat().st_mtime, 3, False)
    engine.api.get_state.return_value = _state(_entry("doc.txt", is_deleted=True, revision=3))

    await engine.reconcile()

    assert a.read_text() == "kept" and engine.db.get_file("doc.txt")["server_revision"] == 3
    assert _archive_is_empty(engine)
