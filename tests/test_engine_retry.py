"""A skipped or failed file must be retried on the next sync, not passed over.

reconcile() only stores the server revision it has seen once every entry was
handled or permanently rejected. A retryable skip (network error, HTTP error,
hash mismatch, local I/O error) leaves the stored revision unchanged, so the
next reconcile fetches the same entries again. Permanent rejections (unsafe
path, malformed entry) must not hold the revision back.

initial_sync() applies the same rule through summary["failed"].
"""
import hashlib
import os

import httpx
import pytest
from unittest.mock import AsyncMock, call

from client.db import LocalDB
from client.engine import SyncEngine

PAYLOAD = b"server payload"
GOOD_SHA = hashlib.sha256(PAYLOAD).hexdigest()

STORED_REV = 1  # revision the client has already seen
LATEST_REV = 5  # revision reported by the (mock) server


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


def _entry(path, **overrides):
    entry = {"path": path, "revision": 2, "is_deleted": False, "sha256": GOOD_SHA}
    entry.update(overrides)
    return entry


def _state(*files):
    return {"files": list(files), "latest_revision": LATEST_REV}


def _status_error(code):
    request = httpx.Request("GET", "http://server.invalid/file")
    response = httpx.Response(code, request=request)
    return httpx.HTTPStatusError(f"HTTP {code}", request=request, response=response)


FAULTS = [
    "http-503",
    "connect-error",
    "read-timeout",
    "hash-mismatch",
    pytest.param(
        "write-oserror",
        marks=pytest.mark.skipif(os.geteuid() == 0, reason="root ignores directory permissions"),
    ),
]


def _inject(engine, root, fault):
    """Break the download (or the local write) of one file.

    Returns (path_of_the_failing_entry, heal) where heal() removes the fault.
    """
    if fault == "write-oserror":
        locked = root / "locked"
        locked.mkdir()
        locked.chmod(0o500)
        return "locked/flaky.txt", lambda: locked.chmod(0o700)

    state = {"broken": True}

    async def download(path):
        if path == "flaky.txt" and state["broken"]:
            if fault == "http-503":
                raise _status_error(503)
            if fault == "connect-error":
                raise httpx.ConnectError("boom")
            if fault == "read-timeout":
                raise httpx.ReadTimeout("slow")
            if fault == "hash-mismatch":
                return b"tampered content"
        return PAYLOAD

    engine.api.download_file.side_effect = download

    def heal():
        state["broken"] = False

    return "flaky.txt", heal


def _since_revisions(engine):
    return [c.args[0] for c in engine.api.get_state.await_args_list]


def _retry_warnings(capsys):
    return [line for line in capsys.readouterr().out.splitlines() if "will be retried" in line]


# --------------------------------------------------------------------------
# reconcile: a retryable skip keeps the revision, the next reconcile retries
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", FAULTS)
async def test_reconcile_retryable_failure_is_retried_on_next_reconcile(make_engine, root, capsys, fault):
    engine = make_engine()
    flaky_path, heal = _inject(engine, root, fault)
    engine.api.get_state.return_value = _state(_entry("good.txt"), _entry(flaky_path))

    try:
        await engine.reconcile()

        # The healthy entry is written, the failing one is not, the revision stays.
        assert (root / "good.txt").read_bytes() == PAYLOAD
        assert not (root / flaky_path).exists()
        assert engine.db.get_last_server_revision() == STORED_REV
        warnings = _retry_warnings(capsys)
        assert len(warnings) == 1 and "1 entry will be retried" in warnings[0]
    finally:
        heal()

    engine.api.download_file.reset_mock()
    await engine.reconcile()

    # The second reconcile starts from the unchanged revision, skips the file that
    # is already up to date (idempotent) and fetches only the one that failed.
    assert _since_revisions(engine) == [STORED_REV, STORED_REV]
    assert engine.api.download_file.await_args_list == [call(flaky_path)]
    assert (root / flaky_path).read_bytes() == PAYLOAD
    assert (root / "good.txt").read_bytes() == PAYLOAD
    assert engine.db.get_last_server_revision() == LATEST_REV
    assert _retry_warnings(capsys) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["existing-directory", "parent-is-file"])
async def test_reconcile_local_conflict_with_directory_or_file_is_retried(make_engine, root, kind):
    """Any other failure caught by the per-entry guard is retryable too."""
    engine = make_engine()
    if kind == "existing-directory":
        (root / "adir").mkdir()
        (root / "adir" / "inner.txt").write_text("inner")
        bad = _entry("adir")
    else:
        (root / "afile.txt").write_text("i am a file")
        bad = _entry("afile.txt/child.txt")
    engine.api.get_state.return_value = _state(bad, _entry("good.txt"))

    await engine.reconcile()

    assert (root / "good.txt").read_bytes() == PAYLOAD
    assert engine.db.get_last_server_revision() == STORED_REV


@pytest.mark.asyncio
async def test_reconcile_failed_deleted_entry_is_retried(make_engine, root):
    engine = make_engine()
    (root / "adir").mkdir()
    # The DB tracks a file here, so the deletion is applied; the directory cannot be hashed.
    engine.db.upsert_file("adir", "0" * 64, 1, 1.0, 1, False)
    engine.api.get_state.return_value = _state(_entry("adir", revision=3, is_deleted=True))

    await engine.reconcile()  # hashing a directory fails

    assert engine.db.get_last_server_revision() == STORED_REV


@pytest.mark.asyncio
async def test_reconcile_retry_warning_counts_entries(make_engine, root, capsys):
    engine = make_engine()
    engine.api.download_file.side_effect = httpx.ConnectError("offline")
    engine.api.get_state.return_value = _state(_entry("a.txt"), _entry("b.txt"), _entry("c.txt"))

    await engine.reconcile()

    warnings = _retry_warnings(capsys)
    assert len(warnings) == 1 and "3 entries will be retried" in warnings[0]
    assert engine.db.get_last_server_revision() == STORED_REV


# --------------------------------------------------------------------------
# reconcile: permanent skips must not hold the revision back
# --------------------------------------------------------------------------

PERMANENT_KINDS = [
    "parent-traversal",
    "absolute-path",
    "symlinked-parent",
    "int-path",
    "bytes-path",
    "missing-path-key",
    "not-a-dict",
    "missing-revision",
    "deleted-missing-revision",
]


def _permanent(root, tmp_path, kind):
    if kind == "parent-traversal":
        return _entry("../evil.txt")
    if kind == "absolute-path":
        return _entry(str(tmp_path / "abs_evil.txt"))
    if kind == "symlinked-parent":
        outside = tmp_path / "outside"
        outside.mkdir()
        (root / "link").symlink_to(outside)
        return _entry("link/pwn.txt")
    if kind == "int-path":
        return _entry(42)
    if kind == "bytes-path":
        return _entry(b"a/b")
    if kind == "missing-path-key":
        entry = _entry("x")
        del entry["path"]
        return entry
    if kind == "not-a-dict":
        return "just-a-string"
    if kind == "missing-revision":
        entry = _entry("no_rev.txt")
        del entry["revision"]
        return entry
    if kind == "deleted-missing-revision":
        entry = _entry("no_rev.txt", is_deleted=True)
        del entry["revision"]
        return entry
    raise AssertionError(kind)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", PERMANENT_KINDS)
async def test_reconcile_permanent_skip_still_advances_revision(make_engine, root, tmp_path, capsys, kind):
    engine = make_engine()
    engine.api.get_state.return_value = _state(_permanent(root, tmp_path, kind), _entry("good.txt"))

    await engine.reconcile()

    assert (root / "good.txt").read_bytes() == PAYLOAD
    assert engine.db.get_last_server_revision() == LATEST_REV
    assert _retry_warnings(capsys) == []
    engine.api.download_file.assert_awaited_once_with("good.txt")


@pytest.mark.asyncio
async def test_reconcile_symlink_skip_is_permanent(make_engine, root):
    engine = make_engine()  # replace_symlinks=False: a local symlink is left alone on purpose
    (root / "target.txt").write_text("hello")
    (root / "link.txt").symlink_to("target.txt")
    engine.api.get_state.return_value = _state(_entry("link.txt"))

    await engine.reconcile()

    engine.api.download_file.assert_not_awaited()
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
async def test_reconcile_up_to_date_and_ignored_entries_advance(make_engine, root):
    engine = make_engine()
    (root / "same.txt").write_bytes(PAYLOAD)
    engine.db.upsert_file("same.txt", GOOD_SHA, len(PAYLOAD), (root / "same.txt").stat().st_mtime, 2, False)
    engine.api.get_state.return_value = _state(_entry("same.txt"), _entry("old.conflict.1.txt"))

    await engine.reconcile()

    engine.api.download_file.assert_not_awaited()
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
@pytest.mark.parametrize("sha", [None, ""])
async def test_reconcile_missing_or_empty_sha256_skips_verification(make_engine, root, sha):
    engine = make_engine()
    engine.api.download_file.return_value = b"anything at all"
    entry = _entry("nohash.txt")
    if sha is None:
        del entry["sha256"]
    else:
        entry["sha256"] = sha
    engine.api.get_state.return_value = _state(entry)

    await engine.reconcile()

    assert (root / "nohash.txt").read_bytes() == b"anything at all"
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
async def test_reconcile_mixed_permanent_and_retryable_does_not_advance(make_engine, root, tmp_path, capsys):
    engine = make_engine()

    async def download(path):
        if path == "flaky_http.txt":
            raise _status_error(503)
        if path == "flaky_net.txt":
            raise httpx.ConnectError("offline")
        return PAYLOAD

    engine.api.download_file.side_effect = download
    engine.api.get_state.return_value = _state(
        _entry("../evil.txt"),
        _permanent(root, tmp_path, "missing-revision"),
        _entry("flaky_http.txt"),
        _entry("good_a.txt"),
        _entry("flaky_net.txt"),
        _entry("good_b.txt"),
    )

    await engine.reconcile()

    # Valid entries are still written; only the two transient failures count.
    assert (root / "good_a.txt").read_bytes() == PAYLOAD
    assert (root / "good_b.txt").read_bytes() == PAYLOAD
    assert not (root / "flaky_http.txt").exists() and not (root / "flaky_net.txt").exists()
    assert not (tmp_path / "evil.txt").exists()
    assert engine.db.get_last_server_revision() == STORED_REV
    warnings = _retry_warnings(capsys)
    assert len(warnings) == 1 and "2 entries will be retried" in warnings[0]


# --------------------------------------------------------------------------
# initial_sync: failed files keep the stored revision, so reconcile retries
# --------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["http-503", "connect-error", "hash-mismatch"])
async def test_initial_sync_failed_download_keeps_stored_revision(make_engine, root, fault):
    engine = make_engine(initialized=False, stored_revision=3)
    _inject(engine, root, fault)
    engine.api.get_state.return_value = _state(_entry("good.txt"), _entry("flaky.txt"))

    summary = await engine.initial_sync()

    assert summary["failed"] == 1 and summary["downloaded"] == 1
    assert engine.db.is_initialized()
    assert engine.db.get_last_server_revision() == 3


@pytest.mark.asyncio
async def test_initial_sync_failed_upload_keeps_stored_revision(make_engine, root):
    engine = make_engine(initialized=False, stored_revision=3)
    (root / "local.txt").write_text("only here")
    engine.api.upload_file.side_effect = httpx.ConnectError("offline")
    engine.api.get_state.return_value = _state()

    summary = await engine.initial_sync()

    assert summary["failed"] == 1
    assert engine.db.is_initialized()
    assert engine.db.get_last_server_revision() == 3


@pytest.mark.asyncio
async def test_initial_sync_failure_is_retried_by_next_reconcile_from_revision_zero(make_engine, root):
    engine = make_engine(initialized=False, stored_revision=0)
    flaky_path, heal = _inject(engine, root, "http-503")
    engine.api.get_state.return_value = _state(_entry("good.txt"), _entry(flaky_path))

    summary = await engine.initial_sync()
    assert summary["failed"] == 1
    assert engine.db.get_last_server_revision() == 0

    heal()
    engine.api.download_file.reset_mock()
    await engine.reconcile()

    # initial_sync always asks from 0; the reconcile must ask from the stored 0 too.
    assert _since_revisions(engine) == [0, 0]
    assert engine.api.download_file.await_args_list == [call(flaky_path)]
    assert (root / flaky_path).read_bytes() == PAYLOAD
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
async def test_initial_sync_without_failures_advances_revision(make_engine, root):
    engine = make_engine(initialized=False, stored_revision=0)
    (root / "local.txt").write_text("only here")
    engine.api.get_state.return_value = _state(_entry("server.txt"))

    summary = await engine.initial_sync()

    assert summary["failed"] == 0 and summary["downloaded"] == 1 and summary["uploaded"] == 1
    assert engine.db.is_initialized()
    assert engine.db.get_last_server_revision() == LATEST_REV


@pytest.mark.asyncio
async def test_initial_sync_unsafe_entry_is_not_a_failure_and_advances(make_engine, root):
    engine = make_engine(initialized=False, stored_revision=0)
    engine.api.get_state.return_value = _state(_entry("../evil.txt"), _entry("good.txt"))

    summary = await engine.initial_sync()

    assert summary["failed"] == 0
    assert engine.db.get_last_server_revision() == LATEST_REV
