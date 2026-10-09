"""reconcile() must never destroy a local file it has no record of, or has changed.

Two ways it used to:

* A non-deleted server entry that lands on a local file with no DB row (e.g.
  after a failed initial_sync, which leaves the stored revision unchanged) was
  downloaded straight over the local file, with no archive and no conflict copy.
* A server deletion was applied to whatever file sat at that path, so a file
  the user had re-created (or edited) since was archived and unlinked, and
  because a retryable skip keeps the stored revision, the same deletion came
  back on every following reconcile.
"""
import asyncio
import hashlib
import os

import httpx
import pytest
from unittest.mock import AsyncMock, call, patch

from client.db import LocalDB
from client.engine import SyncEngine

PAYLOAD = b"server payload"
GOOD_SHA = hashlib.sha256(PAYLOAD).hexdigest()

STORED_REV = 1
LATEST_REV = 5


@pytest.fixture
def root(tmp_path):
    r = tmp_path / "sync"
    r.mkdir()
    return r


@pytest.fixture
def make_engine(root):
    def _make(replace_symlinks=False, initialized=True, stored_revision=STORED_REV):
        db = LocalDB(root / ".syncme" / "local_state.db")
        db.set_initialized(initialized)
        db.set_last_server_revision(stored_revision)
        api = AsyncMock()
        api.download_file.return_value = PAYLOAD
        api.upload_file.return_value = {"revision": 9}
        return SyncEngine(root, db, api, replace_symlinks=replace_symlinks)

    return _make


@pytest.fixture
def notify():
    with patch("client.engine.handle_conflict_notification") as m:
        yield m


def _entry(path, **overrides):
    entry = {"path": path, "revision": 2, "is_deleted": False, "sha256": GOOD_SHA}
    entry.update(overrides)
    return entry


def _state(*files):
    return {"files": list(files), "latest_revision": LATEST_REV}


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _status_error(code):
    request = httpx.Request("GET", "http://server.invalid/file")
    return httpx.HTTPStatusError(f"HTTP {code}", request=request, response=httpx.Response(code, request=request))


def _track(engine, root, name, content: bytes, revision=1):
    """Write a file and record it in the DB exactly as a finished sync would."""
    f = root / name
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_bytes(content)
    st = f.stat()
    engine.db.upsert_file(name, _sha(content), st.st_size, st.st_mtime, revision, False)
    return f


def _archive_is_empty(root):
    archive = root / ".syncme" / "archive"
    return not archive.exists() or not any(archive.iterdir())


def _warnings(capsys):
    return capsys.readouterr().out


def _archived_texts(root):
    archive = root / ".syncme" / "archive"
    if not archive.exists():
        return []
    return [p.read_text() for p in archive.rglob("*") if p.is_file()]


# --------------------------------------------------------------------------
# non-deleted entry on a local file with no DB row
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_untracked_file_with_equal_hash_is_adopted_without_download(make_engine, root, notify):
    engine = make_engine()
    f = root / "doc.txt"
    f.write_bytes(PAYLOAD)
    engine.api.get_state.return_value = _state(_entry("doc.txt"))

    await engine.reconcile()

    engine.api.download_file.assert_not_awaited()
    notify.assert_not_called()
    assert f.read_bytes() == PAYLOAD
    assert list(root.glob("*.conflict.*")) == []
    row = engine.db.get_file("doc.txt")
    assert row["sha256"] == GOOD_SHA and row["server_revision"] == 2
    assert engine.db.get_last_server_revision() == LATEST_REV
    # Adopted for real: the local-changes step must not upload it again.
    engine.api.upload_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_untracked_file_with_different_hash_goes_through_conflict_flow(make_engine, root, notify):
    engine = make_engine()
    f = root / "doc.txt"
    f.write_text("my local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))

    seen = {}

    async def download(path):
        # The fetch happens before anything local is touched.
        seen["local"] = f.read_text()
        seen["conflicts"] = list(root.glob("*.conflict.*"))
        return PAYLOAD

    engine.api.download_file.side_effect = download

    await engine.reconcile()
    await asyncio.gather(*engine._tasks)

    assert seen == {"local": "my local edit", "conflicts": []}
    assert f.read_bytes() == PAYLOAD
    conflicts = list(root.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1 and conflicts[0].read_text() == "my local edit"
    notify.assert_called_once()
    assert notify.call_args.kwargs["conflict_file"] == conflicts[0]
    assert notify.call_args.kwargs["server_file"] == f
    row = engine.db.get_file("doc.txt")
    assert row["sha256"] == GOOD_SHA and row["server_revision"] == 2
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["absent", "empty"])
async def test_untracked_file_with_unknown_server_hash_is_a_conflict(make_engine, root, notify, missing):
    engine = make_engine()
    f = root / "doc.txt"
    f.write_bytes(PAYLOAD)  # equal bytes, but the server gave no hash to prove it
    entry = _entry("doc.txt")
    if missing == "absent":
        del entry["sha256"]
    else:
        entry["sha256"] = ""
    engine.api.get_state.return_value = _state(entry)

    await engine.reconcile()
    await asyncio.gather(*engine._tasks)

    engine.api.download_file.assert_awaited_once_with("doc.txt")
    conflicts = list(root.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1 and conflicts[0].read_bytes() == PAYLOAD
    assert f.read_bytes() == PAYLOAD
    notify.assert_called_once()
    assert engine.db.get_file("doc.txt")["server_revision"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["hash-mismatch", "network", "http-503"])
async def test_untracked_file_failed_fetch_leaves_local_file_untouched(make_engine, root, notify, fault):
    engine = make_engine()
    f = root / "doc.txt"
    f.write_text("my local edit")
    engine.api.get_state.return_value = _state(_entry("doc.txt"))
    # The real server refuses the local-changes upload of a path it already has (409).
    engine.api.upload_file.side_effect = _status_error(409)
    state = {"broken": True}

    async def download(path):
        if state["broken"]:
            if fault == "hash-mismatch":
                return b"tampered content"
            raise httpx.ConnectError("boom") if fault == "network" else _status_error(503)
        return PAYLOAD

    engine.api.download_file.side_effect = download

    await engine.reconcile()

    assert f.read_text() == "my local edit"
    assert list(root.glob("*.conflict.*")) == []
    assert _archive_is_empty(root)
    notify.assert_not_called()
    assert engine.db.get_file("doc.txt") is None
    assert engine.db.get_last_server_revision() == STORED_REV  # retryable: revision held

    # Once the server answers properly the retry resolves it through the conflict flow.
    state["broken"] = False
    await engine.reconcile()
    await asyncio.gather(*engine._tasks)

    assert f.read_bytes() == PAYLOAD
    conflicts = list(root.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1 and conflicts[0].read_text() == "my local edit"
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
async def test_untracked_symlink_is_not_treated_as_a_local_file(make_engine, root, notify):
    """A symlink holds no user data of its own: replace_symlinks keeps its old meaning."""
    engine = make_engine(replace_symlinks=True)
    target = root / "target.txt"
    target.write_text("hello")
    link = root / "link.txt"
    link.symlink_to("target.txt")
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    await engine.reconcile()

    assert not link.is_symlink() and link.read_bytes() == PAYLOAD
    assert target.read_text() == "hello"
    assert list(root.glob("*.conflict.*")) == []
    notify.assert_not_called()


@pytest.mark.asyncio
async def test_untracked_symlink_is_left_alone_without_replace_symlinks(make_engine, root, notify):
    engine = make_engine(replace_symlinks=False)
    (root / "target.txt").write_text("hello")
    (root / "link.txt").symlink_to("target.txt")
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    await engine.reconcile()

    engine.api.download_file.assert_not_awaited()
    assert (root / "link.txt").is_symlink()
    assert list(root.glob("*.conflict.*")) == []
    notify.assert_not_called()
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["l", "ask"])
async def test_failed_initial_sync_then_reconcile_keeps_the_local_version(make_engine, root, notify, choice):
    """The first hazard: initial_sync fails, the revision stays 0, reconcile replays the entry."""
    engine = make_engine(initialized=False, stored_revision=0)
    f = root / "doc.txt"
    f.write_text("my local edit")
    (root / "other.txt").write_text("another local file")
    server = [_entry("doc.txt", revision=2)]

    async def get_state(since):
        return {"files": [e for e in server if e["revision"] > since], "latest_revision": LATEST_REV}

    engine.api.get_state.side_effect = get_state
    engine.api.upload_file.side_effect = httpx.ConnectError("offline")

    summary = await engine.initial_sync(lambda *a: choice)
    assert summary["failed"] >= 1
    assert engine.db.get_last_server_revision() == 0
    assert engine.db.get_file("doc.txt") is None

    await engine.reconcile()
    await asyncio.gather(*engine._tasks)

    # The user's text is never lost: it survives as the conflict copy.
    conflicts = list(root.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1 and conflicts[0].read_text() == "my local edit"
    assert f.read_bytes() == PAYLOAD
    notify.assert_called_once()


# --------------------------------------------------------------------------
# deleted entry
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_delete_of_tracked_unchanged_file_is_applied(make_engine, root, capsys):
    engine = make_engine()
    f = _track(engine, root, "gone.txt", b"old content")
    engine.api.get_state.return_value = _state(_entry("gone.txt", revision=3, is_deleted=True))

    await engine.reconcile()

    assert not f.exists()
    assert engine.db.get_file("gone.txt") is None
    archived = [p for p in (root / ".syncme" / "archive").rglob("*") if p.is_file()]
    assert len(archived) == 1 and archived[0].read_bytes() == b"old content"
    assert engine.db.get_last_server_revision() == LATEST_REV
    assert "not deleting" not in _warnings(capsys)


@pytest.mark.asyncio
async def test_delete_of_tracked_file_that_is_already_gone_drops_the_row(make_engine, root):
    engine = make_engine()
    f = _track(engine, root, "gone.txt", b"old content")
    f.unlink()
    engine.api.get_state.return_value = _state(_entry("gone.txt", revision=3, is_deleted=True))

    await engine.reconcile()

    assert engine.db.get_file("gone.txt") is None
    assert engine.db.get_last_server_revision() == LATEST_REV
    engine.api.delete_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_of_tracked_but_modified_file_is_skipped_with_a_warning(make_engine, root, capsys):
    engine = make_engine()
    f = _track(engine, root, "doc.txt", b"synced content")
    f.write_bytes(b"edited after the last sync")
    st = f.stat()
    # An older recorded mtime is what makes the local-changes step see the edit.
    engine.db.upsert_file("doc.txt", _sha(b"synced content"), 14, st.st_mtime - 100, 1, False)
    engine.api.get_state.return_value = _state(_entry("doc.txt", revision=3, is_deleted=True))

    await engine.reconcile()

    assert f.read_bytes() == b"edited after the last sync"
    assert _archive_is_empty(root)
    out = _warnings(capsys)
    assert out.count("not deleting 'doc.txt'") == 1
    # The row survives, so the normal local-changes step still sees the edit.
    engine.api.upload_file.assert_awaited_once()
    assert engine.api.upload_file.await_args.kwargs["expected_revision"] == 1
    assert engine.db.get_file("doc.txt")["server_revision"] == 9  # mock server's answer to the upload
    assert engine.db.get_last_server_revision() == LATEST_REV  # permanent skip: no retry loop


@pytest.mark.asyncio
async def test_delete_of_modified_file_keeps_the_row_when_the_upload_fails(make_engine, root):
    engine = make_engine()
    f = _track(engine, root, "doc.txt", b"synced content")
    f.write_bytes(b"edited after the last sync")
    st = f.stat()
    engine.db.upsert_file("doc.txt", _sha(b"synced content"), 14, st.st_mtime - 100, 1, False)
    engine.api.upload_file.side_effect = httpx.ConnectError("offline")
    engine.api.get_state.return_value = _state(_entry("doc.txt", revision=3, is_deleted=True))

    await engine.reconcile()

    row = engine.db.get_file("doc.txt")
    assert row is not None and row["server_revision"] == 1 and row["sha256"] == _sha(b"synced content")
    assert f.read_bytes() == b"edited after the last sync"


@pytest.mark.asyncio
async def test_delete_with_no_db_row_is_skipped_with_a_warning(make_engine, root, capsys):
    engine = make_engine()
    f = root / "mine.txt"
    f.write_text("created here, never synced")
    engine.api.upload_file.side_effect = httpx.ConnectError("offline")
    engine.api.get_state.return_value = _state(_entry("mine.txt", revision=3, is_deleted=True))

    await engine.reconcile()

    assert f.read_text() == "created here, never synced"
    assert _archive_is_empty(root)
    assert _warnings(capsys).count("not deleting 'mine.txt'") == 1
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
async def test_delete_of_a_path_that_does_not_exist_locally_is_quiet(make_engine, root, capsys):
    engine = make_engine()
    engine.api.get_state.return_value = _state(_entry("never_here.txt", revision=3, is_deleted=True))

    await engine.reconcile()

    assert "not deleting" not in _warnings(capsys)
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
async def test_failed_delete_of_tracked_unchanged_file_is_retried(make_engine, root):
    engine = make_engine()
    f = _track(engine, root, "gone.txt", b"old content")
    engine.api.get_state.return_value = _state(_entry("gone.txt", revision=3, is_deleted=True))

    with patch("client.engine.archive_file", side_effect=OSError("disk full")):
        await engine.reconcile()

    assert f.read_bytes() == b"old content"
    assert engine.db.get_file("gone.txt") is not None
    assert engine.db.get_last_server_revision() == STORED_REV


@pytest.mark.asyncio
@pytest.mark.parametrize("replays", [1, 3, 5])
async def test_recreated_file_survives_replays_of_the_same_deletion(make_engine, root, replays):
    """The second hazard: a retryable skip pins the revision, so the deletion comes back."""
    engine = make_engine(stored_revision=2)
    _track(engine, root, "x.txt", b"old x")
    server = [
        _entry("x.txt", revision=3, is_deleted=True),
        _entry("y.txt", revision=4),
    ]

    async def get_state(since):
        return {"files": [e for e in server if e["revision"] > since], "latest_revision": 4}

    async def download(path):
        raise httpx.ConnectError("y.txt is unreachable")  # keeps the revision pinned

    engine.api.get_state.side_effect = get_state
    engine.api.download_file.side_effect = download
    engine.api.upload_file.side_effect = httpx.ConnectError("offline")  # x is not uploaded yet

    await engine.reconcile()  # the genuine deletion of the tracked, unchanged x.txt
    assert not (root / "x.txt").exists()
    assert engine.db.get_last_server_revision() == 2

    f = root / "x.txt"
    f.write_text("my brand new x")
    for _ in range(replays):
        await engine.reconcile()
        assert f.read_text() == "my brand new x"
    assert engine.db.get_last_server_revision() == 2
    assert "my brand new x" not in _archived_texts(root)


@pytest.mark.asyncio
@pytest.mark.parametrize("replays", [1, 3])
async def test_uploaded_recreated_file_survives_a_stale_replay_of_the_deletion(make_engine, root, replays):
    """Once x.txt is tracked at a newer revision, the older deletion must not touch it."""
    engine = make_engine(stored_revision=2)
    _track(engine, root, "x.txt", b"old x")
    stale = [_entry("x.txt", revision=3, is_deleted=True), _entry("y.txt", revision=4)]

    async def get_state(since):
        return {"files": [e for e in stale if e["revision"] > since], "latest_revision": 4}

    engine.api.get_state.side_effect = get_state
    engine.api.download_file.side_effect = httpx.ConnectError("y.txt is unreachable")

    await engine.reconcile()
    assert not (root / "x.txt").exists()

    f = root / "x.txt"
    f.write_text("my brand new x")
    # This server keeps replaying the stale deletion even after it accepted the upload.
    for _ in range(replays):
        await engine.reconcile()
        assert f.read_text() == "my brand new x"

    row = engine.db.get_file("x.txt")
    assert row is not None and row["server_revision"] == 9
    assert "my brand new x" not in _archived_texts(root)
