import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from client.engine import SyncEngine
from client.db import LocalDB

@pytest.fixture
def sync_engine(tmp_path):
    db = LocalDB(tmp_path / ".syncme" / "local_state.db")
    db.set_initialized(True)
    api = AsyncMock()
    api.get_state.return_value = {"files": [], "last_revision": 1}
    engine = SyncEngine(tmp_path, db, api)
    return engine

@pytest.mark.asyncio
async def test_reconcile_upload_new_local_file(sync_engine, tmp_path):
    # Create local file
    test_file = tmp_path / "new_local.txt"
    test_file.write_text("content")
    
    sync_engine.api.upload_file.return_value = {"revision": 2}
    
    await sync_engine.reconcile()
    
    sync_engine.api.upload_file.assert_called_once_with("new_local.txt", b"content", expected_revision=0)
    
    # Check db
    f = sync_engine.db.get_file("new_local.txt")
    assert f is not None
    assert f["server_revision"] == 2

@pytest.mark.asyncio
async def test_reconcile_download_new_remote_file(sync_engine, tmp_path):
    sync_engine.api.get_state.return_value = {
        "files": [{"path": "new_remote.txt", "revision": 2, "is_deleted": False}],
        "last_revision": 2
    }
    sync_engine.api.download_file.return_value = b"remote content"
    
    await sync_engine.reconcile()
    
    sync_engine.api.download_file.assert_called_once_with("new_remote.txt")
    
    assert (tmp_path / "new_remote.txt").read_text() == "remote content"
    f = sync_engine.db.get_file("new_remote.txt")
    assert f is not None
    assert f["server_revision"] == 2

@pytest.mark.asyncio
async def test_reconcile_conflict(sync_engine, tmp_path):
    # Local file exists and was modified
    test_file = tmp_path / "conflict.txt"
    test_file.write_text("local content")
    
    # Set older mtime in DB to simulate local modification
    stat = test_file.stat()
    sync_engine.db.upsert_file("conflict.txt", "hash", 10, stat.st_mtime - 100, 1, False)
    
    # Remote also has newer revision
    sync_engine.api.get_state.return_value = {
        "files": [{"path": "conflict.txt", "revision": 2, "is_deleted": False}],
        "last_revision": 2
    }
    sync_engine.api.download_file.return_value = b"remote content"
    
    await sync_engine.reconcile()
    
    # The file should be renamed to .conflict.
    assert test_file.read_text() == "remote content"
    
    conflict_files = list(tmp_path.glob("conflict.conflict.*.txt"))
    assert len(conflict_files) == 1
    assert conflict_files[0].read_text() == "local content"

@pytest.mark.asyncio
async def test_reconcile_ignore_remote_file(sync_engine, tmp_path):
    (tmp_path / ".smignore").write_text("ignored.txt")
    sync_engine.api.get_state.return_value = {
        "files": [{"path": "ignored.txt", "revision": 2, "is_deleted": False}],
        "last_revision": 2
    }
    await sync_engine.reconcile()
    sync_engine.api.download_file.assert_not_called()
    assert sync_engine.db.get_file("ignored.txt") is None

@pytest.mark.asyncio
async def test_reconcile_skip_redownload(sync_engine, tmp_path):
    test_file = tmp_path / "same.txt"
    test_file.write_text("content")
    stat = test_file.stat()
    sync_engine.db.upsert_file("same.txt", "hash", stat.st_size, stat.st_mtime, 1, False)
    
    sync_engine.api.get_state.return_value = {
        "files": [{"path": "same.txt", "revision": 1, "is_deleted": False}],
        "last_revision": 1
    }
    await sync_engine.reconcile()
    sync_engine.api.download_file.assert_not_called()
    
    # Hash matches
    sync_engine.api.get_state.return_value = {
        "files": [{"path": "same.txt", "revision": 2, "is_deleted": False, "sha256": "hash"}],
        "last_revision": 2
    }
    await sync_engine.reconcile()
    sync_engine.api.download_file.assert_not_called()
    assert sync_engine.db.get_file("same.txt")["server_revision"] == 2

@pytest.mark.asyncio
async def test_reconcile_skip_reupload_hash_matches(sync_engine, tmp_path):
    test_file = tmp_path / "upload.txt"
    test_file.write_text("content")
    stat = test_file.stat()
    
    from client.engine import get_file_sha256
    real_hash = get_file_sha256(test_file)
    
    sync_engine.db.upsert_file("upload.txt", real_hash, stat.st_size, stat.st_mtime - 100, 1, False)
    
    await sync_engine.reconcile()
    sync_engine.api.upload_file.assert_not_called()
    f = sync_engine.db.get_file("upload.txt")
    assert f["mtime"] == stat.st_mtime


@pytest.mark.asyncio
async def test_reconcile_conflict_spawns_notification_after_download_and_retains_task(sync_engine, tmp_path):
    import asyncio
    from unittest.mock import patch

    test_file = tmp_path / "race_test.txt"
    test_file.write_text("local draft")
    stat = test_file.stat()
    sync_engine.db.upsert_file("race_test.txt", "old_hash", 10, stat.st_mtime - 100, 1, False)

    sync_engine.api.get_state.return_value = {
        "files": [{"path": "race_test.txt", "revision": 2, "is_deleted": False}],
        "last_revision": 2,
    }
    sync_engine.api.download_file.return_value = b"downloaded server content"

    download_checked = False

    async def fake_handle_conflict(workspace, rel_path, server_file, conflict_file, on_resolve=None, runner=None):
        nonlocal download_checked
        # Verify server file was already downloaded and db already updated
        assert server_file.read_text() == "downloaded server content"
        db_file = sync_engine.db.get_file(rel_path)
        assert db_file["server_revision"] == 2
        download_checked = True
        await asyncio.sleep(0.05)

    with patch("client.engine.handle_conflict_notification", side_effect=fake_handle_conflict):
        await sync_engine.reconcile()
        # Task was retained in _tasks
        assert len(sync_engine._tasks) == 1
        await asyncio.gather(*sync_engine._tasks)
        # Task was discarded after completion
        assert len(sync_engine._tasks) == 0

    assert download_checked is True


@pytest.mark.asyncio
async def test_reconcile_conflict_download_failure_does_not_spawn_notification(sync_engine, tmp_path):
    import httpx
    from unittest.mock import patch

    test_file = tmp_path / "fail_download.txt"
    test_file.write_text("local draft")
    stat = test_file.stat()
    sync_engine.db.upsert_file("fail_download.txt", "old_hash", 10, stat.st_mtime - 100, 1, False)

    sync_engine.api.get_state.return_value = {
        "files": [{"path": "fail_download.txt", "revision": 2, "is_deleted": False}],
        "last_revision": 2,
    }
    sync_engine.api.download_file.side_effect = httpx.ConnectError("Connection refused")
    # The failed download leaves the edited file in place, so the local-changes step now
    # uploads it (it used to be renamed away first, which was the data-loss bug).
    sync_engine.api.upload_file.return_value = {"revision": 3}

    with patch("client.engine.handle_conflict_notification") as mock_notify:
        await sync_engine.reconcile()
        mock_notify.assert_not_called()
        assert len(sync_engine._tasks) == 0


@pytest.mark.asyncio
async def test_reconcile_conflict_resolution_callback_holds_lock(sync_engine, tmp_path):
    import asyncio
    from unittest.mock import patch

    test_file = tmp_path / "lock_test.txt"
    test_file.write_text("local draft")
    stat = test_file.stat()
    sync_engine.db.upsert_file("lock_test.txt", "old_hash", 10, stat.st_mtime - 100, 1, False)

    sync_engine.api.get_state.return_value = {
        "files": [{"path": "lock_test.txt", "revision": 2, "is_deleted": False}],
        "last_revision": 2,
    }
    sync_engine.api.download_file.return_value = b"server content"

    captured_cb = None

    async def fake_handle_conflict(workspace, rel_path, server_file, conflict_file, on_resolve=None, runner=None):
        nonlocal captured_cb
        captured_cb = on_resolve

    with patch("client.engine.handle_conflict_notification", side_effect=fake_handle_conflict):
        await sync_engine.reconcile()
        await asyncio.gather(*sync_engine._tasks)

    assert captured_cb is not None

    lock_held_during_upload = False

    async def fake_upload(path, file_path):
        nonlocal lock_held_during_upload
        lock_held_during_upload = sync_engine._lock.locked()
        return {"revision": 3}

    sync_engine.api.upload_file.side_effect = fake_upload
    await captured_cb("lock_test.txt", "local")

    assert lock_held_during_upload is True
    db_file = sync_engine.db.get_file("lock_test.txt")
    assert db_file["server_revision"] == 3

