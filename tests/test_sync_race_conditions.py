import asyncio
import hashlib
import os
import shutil
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

import client.base_engine as base_engine_module
import client.engine as engine_module
import client.file_engine as file_engine_module
from client.api import APIClient
from client.db import LocalDB
from client.engine import SyncEngine
from client.watcher import Watcher


@pytest.fixture(autouse=True)
def _noop_conflict_notifications(monkeypatch):
    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(engine_module, "handle_conflict_notification", _noop)
    monkeypatch.setattr(file_engine_module, "handle_conflict_notification", _noop)


@pytest.mark.asyncio
async def test_older_mtime_local_modification_is_uploaded_and_not_silently_overwritten(tmp_path):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()
    db = LocalDB(tmp_path / "state.db")
    db.set_initialized(True)

    doc = sync_dir / "doc.txt"
    v1_bytes = b"version_1"
    v1_sha = hashlib.sha256(v1_bytes).hexdigest()
    doc.write_bytes(v1_bytes)
    os.utime(doc, (2000.0, 2000.0))
    st1 = doc.stat()
    db.upsert_file("doc.txt", v1_sha, st1.st_size, st1.st_mtime, 1, False)
    db.set_last_server_revision(1)

    # Case A: Replace local file with same-size content and OLDER mtime (1000.0 < 2000.0)
    v0_bytes = b"version_0"
    doc.write_bytes(v0_bytes)
    os.utime(doc, (1000.0, 1000.0))

    uploaded = []

    class UploadCheckAPI:
        async def get_state(self, since_revision=0):
            return {"files": [], "latest_revision": 1, "last_revision": 1}

        async def upload_file(self, path, content, expected_revision=None):
            uploaded.append((path, content, expected_revision))
            return {"revision": 2}

    engine = SyncEngine(sync_dir, db, UploadCheckAPI())
    await engine.reconcile()

    assert len(uploaded) == 1
    assert uploaded[0] == ("doc.txt", v0_bytes, 1)

    # Case B: Reset to rev 1 (mtime 2000.0), modify locally with older mtime (1000.0) and same size (9),
    # while server advances to rev 2 -> must trigger conflict, NOT overwrite local edit!
    doc.write_bytes(v1_bytes)
    os.utime(doc, (2000.0, 2000.0))
    st_reset = doc.stat()
    db.upsert_file("doc.txt", v1_sha, st_reset.st_size, st_reset.st_mtime, 1, False)
    db.set_last_server_revision(1)

    restored_local = b"restored!"
    doc.write_bytes(restored_local)
    os.utime(doc, (1000.0, 1000.0))

    server_v2 = b"server_v2"
    server_v2_sha = hashlib.sha256(server_v2).hexdigest()

    class ConflictCheckAPI:
        async def get_state(self, since_revision=0):
            return {
                "files": [{"path": "doc.txt", "revision": 2, "sha256": server_v2_sha, "is_deleted": False}],
                "latest_revision": 2,
                "last_revision": 2,
            }

        async def download_file(self, path):
            return server_v2

        async def upload_file(self, path, content, expected_revision=None):
            return {"revision": 3}

    engine2 = SyncEngine(sync_dir, db, ConflictCheckAPI())
    await engine2.reconcile()

    assert doc.read_bytes() == server_v2
    conflicts = list(sync_dir.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1
    assert conflicts[0].read_bytes() == restored_local


@pytest.mark.asyncio
async def test_upload_local_file_atomic_snapshot_prevents_sha_payload_drift(tmp_path, monkeypatch):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()
    db = LocalDB(tmp_path / "state.db")
    db.set_initialized(True)

    doc = sync_dir / "doc.txt"
    v1_bytes = b"v1"
    v1_sha = hashlib.sha256(v1_bytes).hexdigest()
    doc.write_bytes(v1_bytes)
    st1 = doc.stat()
    db.upsert_file("doc.txt", v1_sha, st1.st_size, st1.st_mtime, 1, False)
    db.set_last_server_revision(1)

    doc.write_bytes(b"v2_initial")
    os.utime(doc, (st1.st_mtime + 10.0, st1.st_mtime + 10.0))

    orig_prepare = base_engine_module.prepare_upload_payload

    def _racing_prepare(path: Path, size: int | None = None):
        # Simulate user modifying the file right before payload bytes are read
        if path == doc:
            path.write_bytes(b"v3_modified_mid_upload")
        return orig_prepare(path, size)

    monkeypatch.setattr(base_engine_module, "prepare_upload_payload", _racing_prepare)

    uploaded_payloads = []

    class CaptureUploadAPI:
        async def get_state(self, since_revision=0):
            return {"files": [], "latest_revision": 1, "last_revision": 1}

        async def upload_file(self, path, content, expected_revision=None):
            uploaded_payloads.append(content)
            return {"revision": 2}

    engine = SyncEngine(sync_dir, db, CaptureUploadAPI())
    await engine.reconcile()

    assert len(uploaded_payloads) == 1
    actual_uploaded_sha = hashlib.sha256(uploaded_payloads[0]).hexdigest()
    recorded = db.get_file("doc.txt")
    assert recorded["sha256"] == actual_uploaded_sha
    assert recorded["size"] == len(uploaded_payloads[0])


@pytest.mark.asyncio
async def test_api_client_get_state_and_delete_file_retry_on_transient_transport_errors(monkeypatch):
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    api = APIClient("http://localhost:8000", "test_key", "client_1", workspace_id="ws1")
    try:
        get_attempts = 0

        async def _flaky_get(url, **kwargs):
            nonlocal get_attempts
            get_attempts += 1
            if get_attempts < 3:
                raise httpx.TransportError("transient connection reset")
            req = httpx.Request("GET", url)
            return httpx.Response(200, json={"files": [], "latest_revision": 5}, request=req)

        monkeypatch.setattr(api.client, "get", _flaky_get)
        state = await api.get_state(0)
        assert state == {"files": [], "latest_revision": 5}
        assert get_attempts == 3

        del_attempts = 0

        async def _flaky_delete(url, **kwargs):
            nonlocal del_attempts
            del_attempts += 1
            if del_attempts < 3:
                raise httpx.TimeoutException("transient delete timeout")
            req = httpx.Request("DELETE", url)
            return httpx.Response(200, json={"revision": 6}, request=req)

        monkeypatch.setattr(api.client, "delete", _flaky_delete)
        del_resp = await api.delete_file("doc.txt", expected_revision=5)
        assert del_resp == {"revision": 6}
        assert del_attempts == 3
    finally:
        await api.close()


@pytest.mark.asyncio
async def test_delete_remote_file_catches_transport_error_and_continues_reconcile(tmp_path):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()
    db = LocalDB(tmp_path / "state.db")
    db.set_initialized(True)

    # Seed two files in DB so deleting 1 of 2 does not trip the mass-deletion circuit breaker
    kept = sync_dir / "kept.txt"
    kept.write_bytes(b"keep")
    st_kept = kept.stat()
    db.upsert_file("kept.txt", hashlib.sha256(b"keep").hexdigest(), st_kept.st_size, st_kept.st_mtime, 1, False)

    # deleted.txt is tracked in DB but removed from disk
    db.upsert_file("deleted.txt", hashlib.sha256(b"gone").hexdigest(), 4, st_kept.st_mtime, 1, False)
    db.set_last_server_revision(1)

    # new.txt is newly created on disk and should be uploaded in Step 3 even if Step 2.5 delete fails
    new_file = sync_dir / "new.txt"
    new_file.write_bytes(b"brand new")

    uploaded = []

    class FlakyDeleteAPI:
        async def get_state(self, since_revision=0):
            return {"files": [], "latest_revision": 1, "last_revision": 1}

        async def delete_file(self, path, expected_revision=None):
            raise httpx.TransportError("network unreachable during delete")

        async def upload_file(self, path, content, expected_revision=None):
            uploaded.append(path)
            return {"revision": 2}

    engine = SyncEngine(sync_dir, db, FlakyDeleteAPI())
    await engine.reconcile()

    # deleted.txt must still be in local DB so next reconcile retries deleting it
    assert db.get_file("deleted.txt") is not None
    # Step 3 still ran and uploaded new.txt
    assert uploaded == ["new.txt"]


@pytest.mark.asyncio
async def test_watcher_events_before_get_batches_loop_attachment_are_not_stranded(tmp_path):
    watcher = Watcher(tmp_path, debounce_seconds=0.05)

    # Simulate a watcher whose _loop was previously bound to a now-closed loop (or None)
    closed_loop = asyncio.new_event_loop()
    closed_loop.close()
    watcher._loop = closed_loop

    # Event arrives before get_batches() attaches to the current running loop
    watcher._on_event("early.txt", "modified")

    gen = watcher.get_batches()
    batch = await asyncio.wait_for(anext(gen), timeout=0.5)
    assert ("early.txt", "modified") in batch
    assert watcher.events == set()

    # Subsequent event on the now-attached running loop must also be delivered promptly
    watcher._on_event("subsequent.txt", "modified")
    batch2 = await asyncio.wait_for(anext(gen), timeout=0.5)
    assert ("subsequent.txt", "modified") in batch2


@pytest.mark.asyncio
async def test_watcher_directory_move_emits_events_for_nested_files(tmp_path):
    from watchdog.events import DirMovedEvent
    from client.watcher import SyncEventHandler

    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()
    moved_dir = sync_dir / "moved_dir"
    moved_dir.mkdir()
    nested = moved_dir / "nested.txt"
    nested.write_text("hello from moved dir")

    emitted = []
    handler = SyncEventHandler(sync_dir, lambda rel, ev: emitted.append((rel, ev)))
    handler.on_moved(DirMovedEvent(str(sync_dir / "old_dir"), str(moved_dir)))

    assert ("old_dir", "deleted") in emitted
    assert ("moved_dir/nested.txt", "modified") in emitted
