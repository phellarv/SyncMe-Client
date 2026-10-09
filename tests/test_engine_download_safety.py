"""Data-safety tests for the download paths of SyncEngine.

Two properties are covered:

* Nothing local may change before the downloaded content has been fetched and
  verified (conflict rename, symlink unlink, archive). A hash mismatch or a
  network error must leave the local tree exactly as it was.
* One poisoned server entry must be skipped without stopping the entries that
  follow it.
"""
import asyncio
import hashlib
import os

import httpx
import pytest
from unittest.mock import AsyncMock, patch

from client.db import LocalDB
from client.engine import SyncEngine

PAYLOAD = b"server payload"
GOOD_SHA = hashlib.sha256(PAYLOAD).hexdigest()


@pytest.fixture
def root(tmp_path):
    r = tmp_path / "sync"
    r.mkdir()
    return r


@pytest.fixture
def make_engine(root):
    def _make(replace_symlinks=False, initialized=True):
        db = LocalDB(root / ".syncme" / "local_state.db")
        db.set_initialized(initialized)
        api = AsyncMock()
        api.download_file.return_value = PAYLOAD
        api.upload_file.return_value = {"revision": 9}
        return SyncEngine(root, db, api, replace_symlinks=replace_symlinks)

    return _make


def _entry(path, **overrides):
    entry = {"path": path, "revision": 2, "is_deleted": False, "sha256": GOOD_SHA}
    entry.update(overrides)
    return entry


def _state(*files):
    return {"files": list(files), "latest_revision": 5}


def _break_download(engine, mode):
    """Make the download either fail verification or fail on the network."""
    if mode == "mismatch":
        engine.api.download_file.return_value = b"tampered content"
    else:
        engine.api.download_file.side_effect = httpx.ConnectError("boom")


def _edited_file(engine, root, name="doc.txt", text="local edit"):
    """A synced file that was edited locally since the last sync."""
    f = root / name
    f.write_text(text)
    engine.db.upsert_file(name, "old_hash", 10, f.stat().st_mtime - 100, 1, False)
    # Keep the local-changes upload step from rewriting the DB row.
    engine.api.upload_file.side_effect = httpx.ConnectError("offline")
    return f


def _archive_is_empty(root):
    archive = root / ".syncme" / "archive"
    return not archive.exists() or not any(archive.iterdir())


# --------------------------------------------------------------------------
# reconcile: nothing local changes before the verified download
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["mismatch", "network"])
async def test_reconcile_failed_download_keeps_locally_edited_file(make_engine, root, mode):
    engine = make_engine()
    f = _edited_file(engine, root)
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    _break_download(engine, mode)

    with patch("client.engine.handle_conflict_notification") as notify:
        await engine.reconcile()

    assert f.read_text() == "local edit"
    assert list(root.glob("*.conflict.*")) == []
    notify.assert_not_called()
    assert engine.db.get_file("doc.txt")["server_revision"] == 1


@pytest.mark.asyncio
async def test_reconcile_conflict_fetches_before_touching_local_file(make_engine, root):
    engine = make_engine()
    f = _edited_file(engine, root)
    engine.api.get_state.return_value = _state(_entry("doc.txt"))

    seen = {}

    async def download(path):
        seen["local"] = f.read_text()
        seen["conflicts"] = list(root.glob("*.conflict.*"))
        return PAYLOAD

    engine.api.download_file.side_effect = download

    at_notify = {}

    async def fake_notify(workspace, rel_path, server_file, conflict_file, on_resolve=None, runner=None):
        at_notify["server"] = server_file.read_bytes()
        at_notify["conflict"] = conflict_file.read_text()

    with patch("client.engine.handle_conflict_notification", side_effect=fake_notify):
        await engine.reconcile()
        await asyncio.gather(*engine._tasks)

    # At download time the local file was still untouched.
    assert seen == {"local": "local edit", "conflicts": []}
    # After a verified download: local content moved aside, server content in place.
    assert f.read_bytes() == PAYLOAD
    conflicts = list(root.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1 and conflicts[0].read_text() == "local edit"
    # The notification starts only once both files are final.
    assert at_notify == {"server": PAYLOAD, "conflict": "local edit"}


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["mismatch", "network"])
async def test_reconcile_replace_symlinks_failed_download_keeps_symlink(make_engine, root, mode):
    engine = make_engine(replace_symlinks=True)
    target = root / "target.txt"
    target.write_text("hello")
    link = root / "link.txt"
    link.symlink_to("target.txt")
    engine.api.get_state.return_value = _state(_entry("link.txt"))
    _break_download(engine, mode)

    await engine.reconcile()

    assert link.is_symlink()
    assert os.readlink(link) == "target.txt"
    assert target.read_text() == "hello"


@pytest.mark.asyncio
async def test_reconcile_replace_symlinks_verified_download_replaces_link_only(make_engine, root):
    engine = make_engine(replace_symlinks=True)
    target = root / "target.txt"
    target.write_text("hello")
    link = root / "link.txt"
    link.symlink_to("target.txt")
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    await engine.reconcile()

    assert not link.is_symlink()
    assert link.read_bytes() == PAYLOAD
    assert target.read_text() == "hello"


# --------------------------------------------------------------------------
# initial_sync: nothing local changes before the verified download
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["mismatch", "network"])
@pytest.mark.parametrize("choice", ["server", "both"])
async def test_initial_sync_conflict_failed_download_keeps_local_file(make_engine, root, choice, mode):
    engine = make_engine(initialized=False)
    f = root / "doc.txt"
    f.write_text("local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    _break_download(engine, mode)

    summary = await engine.initial_sync(lambda *a: choice)

    assert f.read_text() == "local edit"
    assert list(root.glob("*.conflict.*")) == []
    assert _archive_is_empty(root)
    assert summary["conflicts"] == 1 and summary["failed"] == 1
    assert engine.db.get_file("doc.txt") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["server", "both"])
async def test_initial_sync_conflict_fetches_before_touching_local_file(make_engine, root, choice):
    engine = make_engine(initialized=False)
    f = root / "doc.txt"
    f.write_text("local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))

    seen = {}

    async def download(path):
        seen["local"] = f.read_text()
        seen["conflicts"] = list(root.glob("*.conflict.*"))
        seen["archive_empty"] = _archive_is_empty(root)
        return PAYLOAD

    engine.api.download_file.side_effect = download

    await engine.initial_sync(lambda *a: choice)

    assert seen == {"local": "local edit", "conflicts": [], "archive_empty": True}
    assert f.read_bytes() == PAYLOAD
    if choice == "both":
        conflicts = list(root.glob("doc.conflict.*.txt"))
        assert len(conflicts) == 1 and conflicts[0].read_text() == "local edit"
    else:
        archived = list((root / ".syncme" / "archive").iterdir())
        assert len(archived) == 1 and archived[0].read_text() == "local edit"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["mismatch", "network"])
async def test_initial_sync_remote_only_replace_symlinks_failed_download_keeps_symlink(make_engine, root, mode):
    engine = make_engine(replace_symlinks=True, initialized=False)
    link = root / "link.txt"
    link.symlink_to("missing.txt")  # broken link: not a local file, so remote-only
    engine.api.get_state.return_value = _state(_entry("link.txt"))
    _break_download(engine, mode)

    summary = await engine.initial_sync()

    assert link.is_symlink() and os.readlink(link) == "missing.txt"
    assert summary["failed"] == 1 and summary["downloaded"] == 0


@pytest.mark.asyncio
async def test_initial_sync_remote_only_replace_symlinks_verified_download_replaces_link(make_engine, root):
    engine = make_engine(replace_symlinks=True, initialized=False)
    link = root / "link.txt"
    link.symlink_to("missing.txt")
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    summary = await engine.initial_sync()

    assert not link.is_symlink() and link.read_bytes() == PAYLOAD
    assert summary["downloaded"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["mismatch", "network"])
@pytest.mark.parametrize("choice", ["server", "both"])
async def test_initial_sync_conflict_replace_symlinks_failed_download_keeps_symlink(make_engine, root, choice, mode):
    engine = make_engine(replace_symlinks=True, initialized=False)
    target = root / "target.txt"
    target.write_text("hello")
    link = root / "link.txt"
    link.symlink_to("target.txt")
    engine.api.get_state.return_value = _state(_entry("link.txt"))
    _break_download(engine, mode)

    await engine.initial_sync(lambda *a: choice)

    assert link.is_symlink() and os.readlink(link) == "target.txt"
    assert target.read_text() == "hello"
    assert list(root.glob("*.conflict.*")) == []
    assert _archive_is_empty(root)


@pytest.mark.asyncio
async def test_initial_sync_server_choice_replace_symlinks_verified_download_replaces_link(make_engine, root):
    engine = make_engine(replace_symlinks=True, initialized=False)
    target = root / "target.txt"
    target.write_text("hello")
    link = root / "link.txt"
    link.symlink_to("target.txt")
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    await engine.initial_sync(lambda *a: "server")

    assert not link.is_symlink() and link.read_bytes() == PAYLOAD
    assert target.read_text() == "hello"


# --------------------------------------------------------------------------
# initial_sync: unsafe paths with a VALID sha256, so safe_join (not the hash
# check) is the only thing standing between the server and the filesystem
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_initial_sync_absolute_path_is_never_fetched_or_written(make_engine, root, tmp_path):
    engine = make_engine(initialized=False)
    victim = tmp_path / "abs_victim.txt"
    engine.api.get_state.return_value = _state(_entry(str(victim)), _entry("good.txt"))

    await engine.initial_sync()

    assert not victim.exists()
    engine.api.download_file.assert_awaited_once_with("good.txt")
    assert (root / "good.txt").read_bytes() == PAYLOAD


@pytest.mark.asyncio
async def test_initial_sync_symlinked_parent_is_never_fetched_or_written(make_engine, root, tmp_path):
    engine = make_engine(initialized=False)
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "link").symlink_to(outside)
    engine.api.get_state.return_value = _state(_entry("link/pwn.txt"), _entry("good.txt"))

    await engine.initial_sync()

    assert not (outside / "pwn.txt").exists()
    engine.api.download_file.assert_awaited_once_with("good.txt")
    assert (root / "good.txt").read_bytes() == PAYLOAD


# --------------------------------------------------------------------------
# poisoned entries: skipped one by one, never abort the sync
# --------------------------------------------------------------------------

COMMON_KINDS = [
    "int-path",
    "bytes-path",
    "missing-path-key",
    "not-a-dict",
    "lone-surrogate",
    "missing-revision",
    "existing-directory",
    "parent-is-file",
]


def _poisoned(root, kind):
    """Return (entry, counted_as_failed_by_initial_sync, path_is_named_in_warning)."""
    if kind == "int-path":
        return _entry(42), False, True
    if kind == "bytes-path":
        return _entry(b"a/b"), False, True
    if kind == "missing-path-key":
        entry = _entry("x")
        del entry["path"]
        return entry, False, False
    if kind == "not-a-dict":
        return None, False, False
    if kind == "lone-surrogate":
        return _entry("a\ud800b"), True, True
    if kind == "missing-revision":
        entry = _entry("no_rev.txt")
        del entry["revision"]
        return entry, True, True
    if kind in ("existing-directory", "deleted-directory"):
        (root / "adir").mkdir()
        (root / "adir" / "inner.txt").write_text("inner")
        return _entry("adir", is_deleted=(kind == "deleted-directory")), True, True
    if kind == "parent-is-file":
        (root / "afile.txt").write_text("i am a file")
        return _entry("afile.txt/child.txt"), True, True
    raise AssertionError(kind)


def _assert_untouched(root, kind):
    if kind in ("existing-directory", "deleted-directory"):
        assert (root / "adir" / "inner.txt").read_text() == "inner"
    if kind == "parent-is-file":
        assert (root / "afile.txt").read_text() == "i am a file"
    if kind == "missing-revision":
        assert not (root / "no_rev.txt").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", COMMON_KINDS + ["deleted-directory"])
async def test_reconcile_poisoned_entry_does_not_stop_later_entries(make_engine, root, capsys, kind):
    engine = make_engine()
    bad, _counted, named = _poisoned(root, kind)
    engine.api.get_state.return_value = _state(bad, _entry("good.txt"))

    await engine.reconcile()

    assert (root / "good.txt").read_bytes() == PAYLOAD
    _assert_untouched(root, kind)
    if named:
        assert repr(bad["path"]) in capsys.readouterr().out


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", COMMON_KINDS)
async def test_initial_sync_poisoned_entry_does_not_stop_later_entries(make_engine, root, capsys, kind):
    engine = make_engine(initialized=False)
    bad, counted, named = _poisoned(root, kind)
    engine.api.get_state.return_value = _state(bad, _entry("good.txt"))

    summary = await engine.initial_sync()

    assert (root / "good.txt").read_bytes() == PAYLOAD
    assert summary["downloaded"] == 1
    assert summary["failed"] == (1 if counted else 0)
    _assert_untouched(root, kind)
    if named:
        assert repr(bad["path"]) in capsys.readouterr().out
    assert engine.db.is_initialized()


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["server", "both"])
async def test_initial_sync_conflict_entry_without_sha256_is_handled(make_engine, root, choice):
    engine = make_engine(initialized=False)
    f = root / "doc.txt"
    f.write_text("local edit")
    no_sha = _entry("doc.txt")
    del no_sha["sha256"]
    engine.api.get_state.return_value = _state(no_sha, _entry("z_good.txt"))

    summary = await engine.initial_sync(lambda *a: choice)

    # A missing server hash never equals the local hash, so it is a conflict;
    # a missing hash skips verification, so the download is written as before.
    assert summary["conflicts"] == 1 and summary["failed"] == 0
    assert f.read_bytes() == PAYLOAD
    assert (root / "z_good.txt").read_bytes() == PAYLOAD


@pytest.mark.asyncio
async def test_initial_sync_entry_without_sha256_default_callback_keeps_local(make_engine, root):
    engine = make_engine(initialized=False)
    f = root / "doc.txt"
    f.write_text("local edit")
    no_sha = _entry("doc.txt")
    del no_sha["sha256"]
    engine.api.get_state.return_value = _state(no_sha)

    summary = await engine.initial_sync()

    assert f.read_text() == "local edit"
    assert summary["conflicts"] == 1 and summary["failed"] == 0


@pytest.mark.asyncio
async def test_initial_sync_conflict_callback_error_aborts_the_sync(make_engine, root):
    """An aborted prompt (e.g. click.Abort on Ctrl-C) is not a poisoned entry."""

    class PromptAborted(Exception):
        pass

    def callback(rel_path, server_file, local_file):
        raise PromptAborted()

    engine = make_engine(initialized=False)
    (root / "doc.txt").write_text("local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))

    with pytest.raises(PromptAborted):
        await engine.initial_sync(callback)

    assert not engine.db.is_initialized()


@pytest.mark.asyncio
async def test_reconcile_cancellation_is_not_swallowed(make_engine, root):
    engine = make_engine()
    engine.api.get_state.return_value = _state(_entry("a.txt"), _entry("b.txt"))
    engine.api.download_file.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await engine.reconcile()


@pytest.mark.asyncio
async def test_initial_sync_cancellation_is_not_swallowed(make_engine, root):
    engine = make_engine(initialized=False)
    engine.api.get_state.return_value = _state(_entry("a.txt"), _entry("b.txt"))
    engine.api.download_file.side_effect = asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await engine.initial_sync()

    assert not engine.db.is_initialized()
