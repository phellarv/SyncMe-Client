import hashlib
from unittest.mock import AsyncMock

import pytest

from client.db import LocalDB
from client.engine import SyncEngine


@pytest.fixture
def env(tmp_path):
    root = tmp_path / "sync"
    root.mkdir()
    db = LocalDB(root / ".syncme" / "local_state.db")
    db.set_initialized(True)
    api = AsyncMock()
    api.download_file.return_value = b"payload"
    engine = SyncEngine(root, db, api)
    return engine, root, tmp_path


def _state(*files):
    return {"files": list(files), "latest_revision": 5}


@pytest.mark.asyncio
async def test_absolute_path_not_written(env):
    engine, root, tmp = env
    target = tmp / "abs_victim.txt"
    engine.api.get_state.return_value = _state(
        {"path": str(target), "revision": 1, "is_deleted": False}
    )
    await engine.reconcile()
    assert not target.exists()
    engine.api.download_file.assert_not_called()


@pytest.mark.asyncio
async def test_dotdot_path_not_written(env):
    engine, root, tmp = env
    engine.api.get_state.return_value = _state(
        {"path": "../escaped.txt", "revision": 1, "is_deleted": False}
    )
    await engine.reconcile()
    assert not (tmp / "escaped.txt").exists()
    engine.api.download_file.assert_not_called()


@pytest.mark.asyncio
async def test_symlinked_parent_not_written(env):
    engine, root, tmp = env
    outside = tmp / "outside"
    outside.mkdir()
    (root / "link").symlink_to(outside)
    engine.api.get_state.return_value = _state(
        {"path": "link/pwn.txt", "revision": 1, "is_deleted": False}
    )
    await engine.reconcile()
    assert not (outside / "pwn.txt").exists()


@pytest.mark.asyncio
async def test_unsafe_delete_does_not_touch_outside_file(env):
    engine, root, tmp = env
    victim = tmp / "victim.txt"
    victim.write_text("keep me")
    engine.api.get_state.return_value = _state(
        {"path": "../victim.txt", "revision": 2, "is_deleted": True}
    )
    await engine.reconcile()
    assert victim.read_text() == "keep me"


@pytest.mark.asyncio
async def test_unsafe_entry_is_skipped_but_valid_entry_still_syncs(env):
    engine, root, tmp = env
    engine.api.get_state.return_value = _state(
        {"path": "../bad.txt", "revision": 1, "is_deleted": False},
        {"path": "good.txt", "revision": 2, "is_deleted": False},
    )
    await engine.reconcile()
    assert (root / "good.txt").read_bytes() == b"payload"
    assert not (tmp / "bad.txt").exists()


@pytest.mark.asyncio
async def test_hash_mismatch_not_written(env):
    engine, root, tmp = env
    engine.api.get_state.return_value = _state(
        {"path": "a.txt", "revision": 1, "is_deleted": False, "sha256": "0" * 64}
    )
    await engine.reconcile()
    assert not (root / "a.txt").exists()
    assert engine.db.get_file("a.txt") is None


@pytest.mark.asyncio
async def test_hash_match_is_written(env):
    engine, root, tmp = env
    engine.api.get_state.return_value = _state(
        {
            "path": "a.txt",
            "revision": 1,
            "is_deleted": False,
            "sha256": hashlib.sha256(b"payload").hexdigest(),
        }
    )
    await engine.reconcile()
    assert (root / "a.txt").read_bytes() == b"payload"


@pytest.mark.asyncio
async def test_initial_sync_skips_unsafe_remote_paths(env):
    engine, root, tmp = env
    engine.db.set_initialized(False)
    engine.api.get_state.return_value = _state(
        {"path": "../bad.txt", "revision": 1, "is_deleted": False, "sha256": "x"},
        {"path": "ok.txt", "revision": 2, "is_deleted": False},
    )
    summary = await engine.initial_sync()
    assert not (tmp / "bad.txt").exists()
    assert (root / "ok.txt").read_bytes() == b"payload"
    assert summary["downloaded"] == 1


@pytest.mark.asyncio
async def test_initial_sync_hash_mismatch_counts_as_failed(env):
    engine, root, tmp = env
    engine.db.set_initialized(False)
    engine.api.get_state.return_value = _state(
        {"path": "a.txt", "revision": 1, "is_deleted": False, "sha256": "0" * 64}
    )
    summary = await engine.initial_sync()
    assert not (root / "a.txt").exists()
    assert summary["failed"] == 1 and summary["downloaded"] == 0
