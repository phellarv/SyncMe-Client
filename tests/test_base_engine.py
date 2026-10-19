import pytest
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch
import httpx
from client.base_engine import BaseEngine, get_file_sha256, prepare_upload_payload
from client.api import STREAM_THRESHOLD

class DummyEngine(BaseEngine):
    def __init__(self, workspace_id, db, api, poll_interval_seconds=300):
        super().__init__(workspace_id, db, api, poll_interval_seconds)
        self.archived = []

    def archive_local_file(self, full_path: Path, rel_path: str):
        self.archived.append((full_path, rel_path))

    async def reconcile(self):
        pass

    async def _watch_loop(self):
        pass


def test_prepare_upload_payload_threshold(tmp_path: Path):
    small_file = tmp_path / "small.txt"
    small_file.write_bytes(b"hello")
    res_small = prepare_upload_payload(small_file)
    assert isinstance(res_small, bytes)
    assert res_small == b"hello"

    large_file = tmp_path / "large.txt"
    large_file.write_bytes(b"A" * (STREAM_THRESHOLD + 10))
    res_large = prepare_upload_payload(large_file)
    assert isinstance(res_large, Path)
    assert res_large == large_file


def test_conflict_path_generation(tmp_path: Path):
    f = tmp_path / "sample.txt"
    f.write_text("v1")
    engine = DummyEngine("test_ws", MagicMock(), MagicMock())
    c1 = engine.generate_conflict_path(f)
    assert "sample.conflict." in c1.name
    assert c1.suffix == ".txt"

    # Rename to conflict
    c_renamed = engine.rename_to_conflict(f)
    assert not f.exists()
    assert c_renamed.exists()


def test_finalize_server_revision():
    db = MagicMock()
    engine = DummyEngine("test_ws", db, MagicMock())
    
    # Skips present -> revision not advanced
    engine.finalize_server_revision(10, retryable_skips=1)
    db.set_last_server_revision.assert_not_called()

    # Zero skips -> revision advanced
    engine.finalize_server_revision(10, retryable_skips=0)
    db.set_last_server_revision.assert_called_once_with(10)


def test_apply_server_deletion(tmp_path: Path):
    db = MagicMock()
    engine = DummyEngine("test_ws", db, MagicMock())

    # 1. Local file doesn't exist on disk -> db.delete_file called
    missing_file = tmp_path / "missing.txt"
    engine.apply_server_deletion("missing.txt", missing_file, remote_rev=5, local_file={"server_revision": 2})
    db.delete_file.assert_called_with("missing.txt")
    db.reset_mock()

    # 2. Stale deletion (local rev >= remote rev) -> do not delete local file or DB
    existing_file = tmp_path / "existing.txt"
    existing_file.write_text("test")
    engine.apply_server_deletion("existing.txt", existing_file, remote_rev=3, local_file={"server_revision": 3})
    assert existing_file.exists()
    db.delete_file.assert_not_called()
    db.reset_mock()

    # 3. Untracked file -> kept as-is, not deleted
    engine.apply_server_deletion("existing.txt", existing_file, remote_rev=5, local_file=None)
    assert existing_file.exists()
    db.delete_file.assert_not_called()
    db.reset_mock()

    # 4. Locally modified file -> kept as-is, not deleted
    engine.apply_server_deletion("existing.txt", existing_file, remote_rev=5, local_file={"server_revision": 2, "sha256": "different_hash"})
    assert existing_file.exists()
    db.delete_file.assert_not_called()
    db.reset_mock()

    # 5. Clean deletion: local file matches DB sha -> archived, unlinked, DB row deleted
    clean_sha = get_file_sha256(existing_file)
    engine.apply_server_deletion("existing.txt", existing_file, remote_rev=5, local_file={"server_revision": 2, "sha256": clean_sha})
    assert not existing_file.exists()
    assert len(engine.archived) == 1
    db.delete_file.assert_called_once_with("existing.txt")


@pytest.mark.asyncio
async def test_delete_remote_file():
    db = MagicMock()
    api = AsyncMock()
    engine = DummyEngine("test_ws", db, api)

    # Success
    await engine._delete_remote_file("doc.txt", expected_rev=3)
    api.delete_file.assert_awaited_once_with("doc.txt", expected_revision=3)
    db.delete_file.assert_called_once_with("doc.txt")
    db.reset_mock()

    # 404 -> still cleans up local DB
    req = httpx.Request("DELETE", "http://server/api")
    resp_404 = httpx.Response(404, request=req)
    api.delete_file.side_effect = httpx.HTTPStatusError("Not Found", request=req, response=resp_404)
    await engine._delete_remote_file("doc.txt", expected_rev=3)
    db.delete_file.assert_called_once_with("doc.txt")
    db.reset_mock()

    # 409 -> ignored (conflict handled by reconcile)
    resp_409 = httpx.Response(409, request=req)
    api.delete_file.side_effect = httpx.HTTPStatusError("Conflict", request=req, response=resp_409)
    await engine._delete_remote_file("doc.txt", expected_rev=3)
    db.delete_file.assert_not_called()


@pytest.mark.asyncio
async def test_conflict_resolution_passes_expected_revision_and_preserves_server_file_on_error(tmp_path: Path):
    from client.db import LocalDB
    db = LocalDB(tmp_path / "state.db")
    db.upsert_file("doc.txt", "sha_server", 6, 100.0, 4, False)
    api = AsyncMock()
    engine = DummyEngine("test_ws", db, api)

    server_file = tmp_path / "doc.txt"
    conflict_file = tmp_path / "doc.conflict.1.txt"
    server_file.write_text("server")
    conflict_file.write_text("local_winner")

    captured_cb = None
    async def fake_notify(workspace, rel_path, server_file, conflict_file, on_resolve=None, runner=None):
        nonlocal captured_cb
        captured_cb = on_resolve

    with patch("client.base_engine.handle_conflict_notification", side_effect=fake_notify):
        engine.record_and_spawn_conflict("doc.txt", server_file, conflict_file)
        await engine._tasks.pop()

    # Simulate failed upload first
    api.upload_file.side_effect = RuntimeError("network down")
    with pytest.raises(RuntimeError):
        await captured_cb("doc.txt", "local")
    assert server_file.read_text() == "server"
    assert conflict_file.read_text() == "local_winner"

    # Now succeed and verify expected_revision=4 was passed
    api.upload_file.side_effect = None
    api.upload_file.return_value = {"revision": 5}
    await captured_cb("doc.txt", "local")
    assert server_file.read_text() == "local_winner"
    assert api.upload_file.call_args.kwargs.get("expected_revision") == 4
    db.close()


@pytest.mark.asyncio
async def test_reconcile_detects_same_mtime_size_change(tmp_path: Path):
    import os
    from client.db import LocalDB
    from client.engine import SyncEngine

    db = LocalDB(tmp_path / ".syncme" / "local_state.db")
    db.set_initialized(True)
    api = AsyncMock()
    api.get_state.return_value = {"files": [], "last_revision": 1}
    api.upload_file.return_value = {"revision": 2}
    engine = SyncEngine(tmp_path, db, api)

    f = tmp_path / "same_mtime.txt"
    f.write_text("short")
    st = f.stat()
    db.upsert_file("same_mtime.txt", get_file_sha256(f), st.st_size, st.st_mtime, 1, False)

    # Modify content and size, but restore exact mtime
    f.write_text("much longer content")
    os.utime(f, (st.st_atime, st.st_mtime))

    await engine.reconcile()
    api.upload_file.assert_called_once()
    db.close()
