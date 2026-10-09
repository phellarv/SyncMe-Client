import pytest
import os
import hashlib
from unittest.mock import AsyncMock, patch
from pathlib import Path
from click.testing import CliRunner

from client.engine import SyncEngine
from client.db import LocalDB
from client.cli import cli

@pytest.fixture
def sync_engine_uninit(tmp_path):
    db = LocalDB(tmp_path / ".syncme" / "local_state.db")
    api = AsyncMock()
    api.get_state.return_value = {"files": [], "last_revision": 0}
    engine = SyncEngine(tmp_path, db, api)
    return engine

@pytest.mark.asyncio
async def test_reconcile_fails_if_not_initialized(sync_engine_uninit):
    with pytest.raises(RuntimeError, match="SyncMe is not initialized"):
        await sync_engine_uninit.reconcile()

@pytest.mark.asyncio
async def test_initial_sync_local_only(sync_engine_uninit, tmp_path):
    test_file = tmp_path / "local.txt"
    test_file.write_text("local")
    
    sync_engine_uninit.api.upload_file.return_value = {"revision": 1}
    
    summary = await sync_engine_uninit.initial_sync()
    
    assert summary["uploaded"] == 1
    assert summary["downloaded"] == 0
    sync_engine_uninit.api.upload_file.assert_called_once_with("local.txt", b"local", expected_revision=0)
    assert sync_engine_uninit.db.is_initialized()

@pytest.mark.asyncio
async def test_initial_sync_server_only(sync_engine_uninit, tmp_path):
    sync_engine_uninit.api.get_state.return_value = {
        "files": [{"path": "server.txt", "revision": 1, "is_deleted": False, "sha256": get_sha256(b"server content")}],
        "last_revision": 1
    }
    sync_engine_uninit.api.download_file.return_value = b"server content"
    
    summary = await sync_engine_uninit.initial_sync()
    
    assert summary["downloaded"] == 1
    assert summary["uploaded"] == 0
    assert (tmp_path / "server.txt").read_text() == "server content"
    assert sync_engine_uninit.db.is_initialized()

def get_sha256(content: bytes) -> str:
    h = hashlib.sha256()
    h.update(content)
    return h.hexdigest()

@pytest.mark.asyncio
async def test_initial_sync_unchanged(sync_engine_uninit, tmp_path):
    content = b"same content"
    sha = get_sha256(content)
    
    test_file = tmp_path / "same.txt"
    test_file.write_bytes(content)
    
    sync_engine_uninit.api.get_state.return_value = {
        "files": [{"path": "same.txt", "revision": 1, "is_deleted": False, "sha256": sha}],
        "last_revision": 1
    }
    
    summary = await sync_engine_uninit.initial_sync()
    
    assert summary["unchanged"] == 1
    assert summary["downloaded"] == 0
    assert summary["uploaded"] == 0
    sync_engine_uninit.api.download_file.assert_not_called()
    sync_engine_uninit.api.upload_file.assert_not_called()

@pytest.mark.asyncio
async def test_initial_sync_conflict_server(sync_engine_uninit, tmp_path):
    test_file = tmp_path / "conflict.txt"
    test_file.write_text("local")
    
    sync_engine_uninit.api.get_state.return_value = {
        "files": [{"path": "conflict.txt", "revision": 1, "is_deleted": False, "sha256": get_sha256(b"server content")}],
        "last_revision": 1
    }
    sync_engine_uninit.api.download_file.return_value = b"server content"
    
    def cb(rel_path, server_file, local_file):
        return "s"
        
    summary = await sync_engine_uninit.initial_sync(conflict_callback=cb)
    
    assert summary["conflicts"] == 1
    assert test_file.read_text() == "server content"
    # Should archive local, but we don't strictly assert the archive path here, 
    # just that server content is placed

@pytest.mark.asyncio
async def test_initial_sync_conflict_local(sync_engine_uninit, tmp_path):
    test_file = tmp_path / "conflict.txt"
    test_file.write_text("local")
    
    sync_engine_uninit.api.get_state.return_value = {
        "files": [{"path": "conflict.txt", "revision": 1, "is_deleted": False, "sha256": "server_sha"}],
        "last_revision": 1
    }
    sync_engine_uninit.api.upload_file.return_value = {"revision": 2}
    
    def cb(rel_path, server_file, local_file):
        return "l"
        
    summary = await sync_engine_uninit.initial_sync(conflict_callback=cb)
    
    assert summary["conflicts"] == 1
    sync_engine_uninit.api.upload_file.assert_called_once_with("conflict.txt", b"local", expected_revision=1)

@pytest.mark.asyncio
async def test_initial_sync_conflict_both(sync_engine_uninit, tmp_path):
    test_file = tmp_path / "conflict.txt"
    test_file.write_text("local")
    
    sync_engine_uninit.api.get_state.return_value = {
        "files": [{"path": "conflict.txt", "revision": 1, "is_deleted": False, "sha256": get_sha256(b"server content")}],
        "last_revision": 1
    }
    sync_engine_uninit.api.download_file.return_value = b"server content"
    
    def cb(rel_path, server_file, local_file):
        return "b"
        
    summary = await sync_engine_uninit.initial_sync(conflict_callback=cb)
    
    assert summary["conflicts"] == 1
    assert test_file.read_text() == "server content"
    
    conflict_files = list(tmp_path.glob("conflict.conflict.*.txt"))
    assert len(conflict_files) == 1
    assert conflict_files[0].read_text() == "local"

def test_cli_run_fails_if_not_initialized(tmp_path, monkeypatch):
    # Setup uninitialized db
    runner = CliRunner()
    from client.config import settings
    # A valid server config, independent of the host's real config file.
    monkeypatch.setattr(settings.server, "api_key", "k" * 43)
    monkeypatch.setattr(settings.server, "url", "http://localhost:8000")
    old_path = settings.directories[0].local_path
    settings.directories[0].local_path = tmp_path
    try:
        result = runner.invoke(cli, ["run"])
        assert result.exit_code == 1
        assert "SyncMe is not initialized" in result.output
    finally:
        settings.directories[0].local_path = old_path

@pytest.mark.asyncio
async def test_initial_sync_ignore_remote(sync_engine_uninit, tmp_path):
    (tmp_path / ".smignore").write_text("ignored.txt")
    sync_engine_uninit.api.get_state.return_value = {
        "files": [{"path": "ignored.txt", "revision": 1, "is_deleted": False, "sha256": "dummy"}],
        "last_revision": 1
    }
    summary = await sync_engine_uninit.initial_sync()
    assert summary["downloaded"] == 0
    sync_engine_uninit.api.download_file.assert_not_called()
