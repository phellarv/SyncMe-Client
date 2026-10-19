import hashlib
import os
from pathlib import Path

import httpx
import pytest

import client.engine as engine_module
import client.file_engine as file_engine_module
from client.base_engine import BaseEngine
from client.config import ClientConfig, ConfigError, DirectoryConfig, FileConfig, ServerConfig
from client.db import LocalDB
from client.engine import SyncEngine
from client.file_engine import FileGroupEngine


@pytest.fixture(autouse=True)
def _noop_conflict_notifications(monkeypatch):
    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(engine_module, "handle_conflict_notification", _noop)
    monkeypatch.setattr(file_engine_module, "handle_conflict_notification", _noop)


def test_config_rejects_duplicate_file_remote_paths_in_same_workspace(tmp_path):
    with pytest.raises(ConfigError, match="Duplicate remote_path"):
        ClientConfig(
            directories=[],
            files=[
                FileConfig(local_path=tmp_path / "appA" / "config.toml", workspace="files"),
                FileConfig(local_path=tmp_path / "appB" / "config.toml", workspace="files"),
            ],
        )

    # Unicode NFD vs NFC normalization collision
    with pytest.raises(ConfigError, match="Duplicate remote_path"):
        ClientConfig(
            directories=[],
            files=[
                FileConfig(local_path=tmp_path / "a.txt", workspace="files", remote_path="cafe\u0301.txt"),
                FileConfig(local_path=tmp_path / "b.txt", workspace="files", remote_path="caf\u00e9.txt"),
            ],
        )


def test_config_rejects_workspace_collision_between_directories_and_files(tmp_path):
    with pytest.raises(ConfigError, match="collides with directory workspace"):
        ClientConfig(
            directories=[DirectoryConfig(name="shared", local_path=tmp_path / "d")],
            files=[FileConfig(local_path=tmp_path / "f.txt", workspace="shared")],
        )


def test_file_group_engine_rejects_duplicate_remote_paths_on_init(tmp_path):
    db = LocalDB(tmp_path / "state.db")
    f1 = FileConfig(local_path=tmp_path / "a" / "same.txt", workspace="ws", remote_path="same.txt")
    f2 = FileConfig(local_path=tmp_path / "b" / "same.txt", workspace="ws", remote_path="same.txt")

    with pytest.raises(ConfigError, match="Duplicate remote_path"):
        FileGroupEngine("ws", [f1, f2], db, object(), ServerConfig())


@pytest.mark.asyncio
async def test_aba_server_revert_with_dirty_local_file_triggers_conflict_not_silent_overwrite(tmp_path):
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

    # User modifies doc.txt locally on disk (mtime advances)
    local_edit = b"local uncommitted edit"
    doc.write_bytes(local_edit)
    os.utime(doc, (st1.st_mtime + 10.0, st1.st_mtime + 10.0))

    uploaded = []

    class ABAAPI:
        async def get_state(self, since_revision=0):
            # Server went v1 (rev 1) -> v2 (rev 2) -> v1 (rev 3)
            return {
                "files": [{"path": "doc.txt", "revision": 3, "sha256": v1_sha, "is_deleted": False}],
                "latest_revision": 3,
                "last_revision": 3,
            }

        async def download_file(self, path):
            return v1_bytes

        async def upload_file(self, path, content, expected_revision=None):
            uploaded.append((path, content, expected_revision))
            return {"revision": 4}

    engine = SyncEngine(sync_dir, db, ABAAPI())
    await engine.reconcile()

    # Must NOT have silently uploaded local_edit over rev 3!
    assert uploaded == []
    # Server content v1 is in doc.txt
    assert doc.read_bytes() == v1_bytes
    # Local dirty edit preserved in a conflict file and recorded in DB
    conflicts = list(sync_dir.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1
    assert conflicts[0].read_bytes() == local_edit
    assert db.count_pending_conflicts() == 1


@pytest.mark.asyncio
async def test_identical_concurrent_edits_converge_without_conflict(tmp_path):
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

    v2_bytes = b"converged v2"
    v2_sha = hashlib.sha256(v2_bytes).hexdigest()
    doc.write_bytes(v2_bytes)
    os.utime(doc, (st1.st_mtime + 10.0, st1.st_mtime + 10.0))

    downloads = []
    uploads = []

    class ConvergedAPI:
        async def get_state(self, since_revision=0):
            return {
                "files": [{"path": "doc.txt", "revision": 2, "sha256": v2_sha, "is_deleted": False}],
                "latest_revision": 2,
                "last_revision": 2,
            }

        async def download_file(self, path):
            downloads.append(path)
            return v2_bytes

        async def upload_file(self, path, content, expected_revision=None):
            uploads.append(path)
            return {"revision": 3}

    engine = SyncEngine(sync_dir, db, ConvergedAPI())
    await engine.reconcile()

    assert downloads == []
    assert uploads == []
    assert list(sync_dir.glob("doc.conflict.*")) == []
    assert db.count_pending_conflicts() == 0
    rec = db.get_file("doc.txt")
    assert rec["server_revision"] == 2
    assert rec["sha256"] == v2_sha


@pytest.mark.asyncio
async def test_local_touch_without_content_change_fast_forwards_remote_update_without_conflict(tmp_path):
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

    # Touch local file without changing content
    os.utime(doc, (st1.st_mtime + 15.0, st1.st_mtime + 15.0))

    v2_bytes = b"v2 from server"
    v2_sha = hashlib.sha256(v2_bytes).hexdigest()

    class FastForwardAPI:
        async def get_state(self, since_revision=0):
            return {
                "files": [{"path": "doc.txt", "revision": 2, "sha256": v2_sha, "is_deleted": False}],
                "latest_revision": 2,
                "last_revision": 2,
            }

        async def download_file(self, path):
            return v2_bytes

        async def upload_file(self, path, content, expected_revision=None):
            return {"revision": 3}

    engine = SyncEngine(sync_dir, db, FastForwardAPI())
    await engine.reconcile()

    assert list(sync_dir.glob("doc.conflict.*")) == []
    assert db.count_pending_conflicts() == 0
    assert doc.read_bytes() == v2_bytes
    assert db.get_file("doc.txt")["server_revision"] == 2


@pytest.mark.asyncio
async def test_upload_409_against_server_tombstone_retries_with_if_match_zero(tmp_path):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()
    db = LocalDB(tmp_path / "state.db")
    db.set_initialized(True)

    doc = sync_dir / "doc.txt"
    v1_bytes = b"v1"
    v1_sha = hashlib.sha256(v1_bytes).hexdigest()
    doc.write_bytes(v1_bytes)
    st1 = doc.stat()
    db.upsert_file("doc.txt", v1_sha, st1.st_size, st1.st_mtime, 2, False)
    db.set_last_server_revision(2)

    # Modify locally
    v2_bytes = b"v2 recreated"
    doc.write_bytes(v2_bytes)
    os.utime(doc, (st1.st_mtime + 10.0, st1.st_mtime + 10.0))

    upload_calls = []

    class Tombstone409API:
        async def get_state(self, since_revision=0):
            return {"files": [], "latest_revision": 2, "last_revision": 2}

        async def upload_file(self, path, content, expected_revision=None):
            upload_calls.append(expected_revision)
            if expected_revision == 2:
                req = httpx.Request("PUT", "http://test/files/doc.txt")
                resp = httpx.Response(
                    409,
                    headers={
                        "X-SyncMe-Conflict-Reason": "deleted",
                        "X-SyncMe-Current-Revision": "0",
                        "X-SyncMe-Head-Revision": "3",
                    },
                    request=req,
                )
                raise httpx.HTTPStatusError("409 Conflict", request=req, response=resp)
            return {"revision": 4}

    engine = SyncEngine(sync_dir, db, Tombstone409API())
    await engine.reconcile()

    assert upload_calls == [2, 0]
    rec = db.get_file("doc.txt")
    assert rec["server_revision"] == 4
    assert rec["sha256"] == hashlib.sha256(v2_bytes).hexdigest()


@pytest.mark.asyncio
async def test_upload_409_revision_mismatch_triggers_immediate_conflict_reconcile(tmp_path):
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

    local_v2 = b"local v2"
    doc.write_bytes(local_v2)
    os.utime(doc, (st1.st_mtime + 10.0, st1.st_mtime + 10.0))

    remote_v2 = b"remote v2"
    remote_v2_sha = hashlib.sha256(remote_v2).hexdigest()

    class Race409API:
        async def get_state(self, since_revision=0):
            return {"files": [], "latest_revision": 1, "last_revision": 1}

        async def upload_file(self, path, content, expected_revision=None):
            req = httpx.Request("PUT", "http://test/files/doc.txt")
            resp = httpx.Response(
                409,
                headers={
                    "X-SyncMe-Conflict-Reason": "revision_mismatch",
                    "X-SyncMe-Current-Revision": "2",
                    "X-SyncMe-Head-Revision": "2",
                    "X-SyncMe-Current-SHA256": remote_v2_sha,
                },
                request=req,
            )
            raise httpx.HTTPStatusError("409 Conflict", request=req, response=resp)

        async def download_file(self, path):
            return remote_v2

    engine = SyncEngine(sync_dir, db, Race409API())
    await engine.reconcile()

    assert doc.read_bytes() == remote_v2
    conflicts = list(sync_dir.glob("doc.conflict.*.txt"))
    assert len(conflicts) == 1
    assert conflicts[0].read_bytes() == local_v2
    assert db.count_pending_conflicts() == 1
    assert db.get_file("doc.txt")["server_revision"] == 2


def test_repeated_conflict_archives_previous_conflict_file_and_bounds_filename_length(tmp_path):
    # 1. Verify filename length is bounded <= 230 bytes even for a 225-byte stem
    long_name = ("a" * 225) + ".txt"
    long_path = tmp_path / long_name
    conflict_path = BaseEngine.generate_conflict_path(long_path)
    assert len(conflict_path.name.encode("utf-8")) <= 230
    assert ".conflict." in conflict_path.name
    assert conflict_path.name.endswith(".txt")

    # 2. Verify repeated conflict on the same file archives the older conflict copy
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()
    db = LocalDB(tmp_path / "state.db")

    server_f = sync_dir / "doc.txt"
    server_f.write_bytes(b"server")
    c1 = sync_dir / "doc.conflict.1000.txt"
    c1.write_bytes(b"conflict version 1")
    c2 = sync_dir / "doc.conflict.2000.txt"
    c2.write_bytes(b"conflict version 2")

    cid1 = db.record_conflict("sync", "doc.txt", str(server_f), str(c1))
    cid2 = db.record_conflict("sync", "doc.txt", str(server_f), str(c2))
    assert cid1 == cid2

    # First conflict file still exists AND was archived into .syncme/archive/
    assert c1.read_bytes() == b"conflict version 1"
    archive_dir = sync_dir / ".syncme" / "archive"
    archived = list(archive_dir.glob("doc.conflict.1000.txt_*"))
    assert len(archived) == 1
    assert archived[0].read_bytes() == b"conflict version 1"
