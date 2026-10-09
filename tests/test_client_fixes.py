"""Tests for client fixes:
1. Mass deletion protection (circuit breaker).
2. Atomic download and TOCTOU conflict preservation.
3. Sliding debounce in Watcher.
"""

import asyncio
import time
from pathlib import Path
import pytest

from client.db import LocalDB
from client.engine import SyncEngine
from client.watcher import Watcher


class DummyAPI:
    def __init__(self, files=None):
        self._files = files or []

    async def get_state(self, since_revision=0):
        return {"files": self._files, "latest_revision": 10, "last_revision": 10}

    async def delete_file(self, path, expected_revision=None):
        return {"revision": 11}

    async def download_file(self, path):
        return b"server remote content"

    async def upload_file(self, path, content, expected_revision=None):
        return {"revision": 12}


# ============================================================================
# Criterion 1: Mass deletion protection
# ============================================================================


@pytest.mark.asyncio
async def test_mass_deletion_protection_aborts_when_dir_missing(tmp_path):
    """If sync_dir does not exist or was unmounted, reconcile must not delete all files."""
    sync_dir = tmp_path / "sync_folder"
    sync_dir.mkdir()
    db_path = tmp_path / "state.db"
    db = LocalDB(db_path)
    db.set_initialized(True)

    # Seed 10 files in DB
    for i in range(10):
        db.upsert_file(f"file_{i}.txt", f"hash_{i}", 100, 1000.0, 1, False)

    # Simulate unmounted sync_dir (directory removed)
    sync_dir.rmdir()

    deleted_on_server = []

    class MockDeleteAPI(DummyAPI):
        async def delete_file(self, path, expected_revision=None):
            deleted_on_server.append(path)
            return {"revision": 11}

    engine = SyncEngine(sync_dir, db, MockDeleteAPI())
    await engine.reconcile()

    # Nothing should have been deleted on the server
    assert len(deleted_on_server) == 0, f"Mass deletion circuit breaker failed! Deleted: {deleted_on_server}"
    # Files should still remain in local db
    assert len(db.get_all_files()) == 10


@pytest.mark.asyncio
async def test_mass_deletion_threshold_aborts_when_all_files_vanish(tmp_path):
    """If all tracked files disappear at once while directory exists, circuit breaker aborts deletions."""
    sync_dir = tmp_path / "sync_folder"
    sync_dir.mkdir()
    db_path = tmp_path / "state.db"
    db = LocalDB(db_path)
    db.set_initialized(True)

    # Seed 10 files in DB (all missing from sync_dir)
    for i in range(10):
        db.upsert_file(f"file_{i}.txt", f"hash_{i}", 100, 1000.0, 1, False)

    deleted_on_server = []

    class MockDeleteAPI(DummyAPI):
        async def delete_file(self, path, expected_revision=None):
            deleted_on_server.append(path)
            return {"revision": 11}

    engine = SyncEngine(sync_dir, db, MockDeleteAPI())
    await engine.reconcile()

    # Circuit breaker must prevent mass wipeout
    assert len(deleted_on_server) == 0, f"Mass deletion threshold failed! Deleted: {deleted_on_server}"
    assert len(db.get_all_files()) == 10


# ============================================================================
# Criterion 2: TOCTOU conflict preservation during download
# ============================================================================


@pytest.mark.asyncio
async def test_toctou_local_change_during_download_creates_conflict(tmp_path):
    """If a file is edited locally while download is in progress, the local edit must become a conflict."""
    sync_dir = tmp_path / "sync_folder"
    sync_dir.mkdir()
    db_path = tmp_path / "state.db"
    db = LocalDB(db_path)
    db.set_initialized(True)

    test_file = sync_dir / "doc.txt"
    test_file.write_text("initial local text")
    stat = test_file.stat()

    import hashlib
    initial_sha = hashlib.sha256(b"initial local text").hexdigest()
    db.upsert_file("doc.txt", initial_sha, stat.st_size, stat.st_mtime, 1, False)

    # Server announces revision 2 for doc.txt
    server_sha = hashlib.sha256(b"server remote content").hexdigest()
    remote_files = [{"path": "doc.txt", "revision": 2, "sha256": server_sha, "is_deleted": False}]

    class SlowDownloadAPI(DummyAPI):
        def __init__(self):
            super().__init__(remote_files)

        async def download_file(self, path):
            # User edits local file while download is in-flight!
            await asyncio.sleep(0.05)
            test_file.write_text("user concurrent modification while downloading")
            return b"server remote content"

    engine = SyncEngine(sync_dir, db, SlowDownloadAPI())
    await engine.reconcile()

    # The server content should now be written to doc.txt
    assert test_file.read_text() == "server remote content"

    # AND a .conflict file must exist containing the user's concurrent edit!
    conflict_files = list(sync_dir.glob("doc.conflict.*"))
    assert len(conflict_files) == 1, "Concurrent edit was lost! No conflict file was created."
    assert conflict_files[0].read_text() == "user concurrent modification while downloading"


# ============================================================================
# Criterion 3: Sliding debounce in Watcher
# ============================================================================


@pytest.mark.asyncio
async def test_sliding_debounce_extends_wait_time(tmp_path):
    """Watcher debounce must reset timer if new events arrive during debounce."""
    sync_dir = tmp_path / "watch_dir"
    sync_dir.mkdir()
    watcher = Watcher(sync_dir, debounce_seconds=0.2)
    watcher.start()

    # Manually feed events to queue with small delays
    queue = watcher._event_queue

    async def feeder():
        for i in range(3):
            await asyncio.sleep(0.1)  # Each delay is < 0.2s debounce
            queue.put_nowait((f"file_{i}.txt", "modified"))

    t0 = time.monotonic()
    asyncio.create_task(feeder())

    batches_gen = watcher.get_batches()
    batch = await anext(batches_gen)
    duration = time.monotonic() - t0

    watcher.stop()
    # 3 events fed at t=0.1, 0.2, 0.3. With sliding debounce of 0.2, batch should fire at ~0.5s
    # With static debounce of 0.2s from first event (t=0.1 + 0.2 = 0.3s), it would fire at ~0.3s
    assert len(batch) == 3, f"Expected 3 items in sliding debounce batch, got {len(batch)}"
    assert duration >= 0.45, f"Batch fired too early ({duration:.2f}s), debounce was not sliding"
