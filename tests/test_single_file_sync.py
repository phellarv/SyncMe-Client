import hashlib
import pytest
from pathlib import Path
from client.config import load_config, FileConfig
from client.file_engine import FileGroupEngine
from client.db import LocalDB

@pytest.fixture
def mock_api():
    class MockAPI:
        def __init__(self):
            self.remote_state = {'last_revision': 0, 'files': []}
            self.files = {}
            self.events = []
            
        async def get_state(self, revision):
            return self.remote_state

        async def download_file(self, path):
            return self.files.get(path, b"")

        async def upload_file(self, path, content, expected_revision=0):
            self.files[path] = content
            return {'revision': expected_revision + 1}
            
        async def delete_file(self, path, expected_revision=0):
            if path in self.files:
                del self.files[path]

        async def listen_events(self):
            for e in self.events:
                yield e
                
        async def close(self):
            pass

    return MockAPI()

@pytest.fixture
def temp_workspace(tmp_path, monkeypatch):
    import os
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path

def test_parse_files_from_toml(temp_workspace, monkeypatch):
    from client.config import load_config
    config_path = temp_workspace / "config.toml"
    config_path.write_text("""
[[files]]
local_path = "~/test.txt"
workspace = "my_files"
    """)
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_path))
    config = load_config()
    assert len(config.files) == 1
    assert config.files[0].workspace == "my_files"
    assert config.files[0].local_path == temp_workspace / "test.txt"
    assert config.files[0].remote_path == "test.txt"

@pytest.mark.asyncio
async def test_initial_sync_upload(temp_workspace, mock_api):
    local_path = temp_workspace / "local.txt"
    local_path.write_text("hello")
    f_conf = FileConfig(local_path=local_path, remote_path="remote.txt")
    
    db_path = temp_workspace / "db.sqlite3"
    db = LocalDB(db_path)
    from client.config import ServerConfig
    
    engine = FileGroupEngine("test_ws", [f_conf], db, mock_api, ServerConfig())
    summary = await engine.initial_sync()
    
    assert summary["uploaded"] == 1
    assert mock_api.files["remote.txt"] == b"hello"
    assert db.is_initialized()

@pytest.mark.asyncio
async def test_initial_sync_download(temp_workspace, mock_api):
    local_path = temp_workspace / "local.txt"
    f_conf = FileConfig(local_path=local_path, remote_path="remote.txt")
    
    mock_api.remote_state['files'] = [{'path': 'remote.txt', 'revision': 1, 'sha256': hashlib.sha256(b"world").hexdigest()}]
    mock_api.files['remote.txt'] = b"world"
    
    db_path = temp_workspace / "db.sqlite3"
    db = LocalDB(db_path)
    from client.config import ServerConfig
    
    engine = FileGroupEngine("test_ws", [f_conf], db, mock_api, ServerConfig())
    summary = await engine.initial_sync()
    
    assert summary["downloaded"] == 1
    assert local_path.read_text() == "world"

@pytest.mark.asyncio
async def test_reconcile_upload_changes(temp_workspace, mock_api):
    local_path = temp_workspace / "local.txt"
    local_path.write_text("hello")
    f_conf = FileConfig(local_path=local_path, remote_path="remote.txt")
    
    db_path = temp_workspace / "db.sqlite3"
    db = LocalDB(db_path)
    from client.config import ServerConfig
    
    engine = FileGroupEngine("test_ws", [f_conf], db, mock_api, ServerConfig())
    await engine.initial_sync()
    
    # Change local
    local_path.write_text("hello world")
    await engine.reconcile()
    
    assert mock_api.files["remote.txt"] == b"hello world"


@pytest.mark.asyncio
async def test_file_engine_reconcile_conflict_spawns_notification_after_download_and_retains_task(temp_workspace, mock_api):
    import asyncio
    from unittest.mock import patch
    from client.config import ServerConfig

    local_path = temp_workspace / "single_conflict.txt"
    local_path.write_text("initial")
    f_conf = FileConfig(local_path=local_path, remote_path="remote_conflict.txt")

    db_path = temp_workspace / "db.sqlite3"
    db = LocalDB(db_path)
    engine = FileGroupEngine("test_ws", [f_conf], db, mock_api, ServerConfig())
    await engine.initial_sync()

    # Modify local file and simulate server revision advance
    local_path.write_text("local edits")
    mock_api.remote_state['files'] = [{'path': 'remote_conflict.txt', 'revision': 2, 'sha256': hashlib.sha256(b"server updated content").hexdigest()}]
    mock_api.remote_state['latest_revision'] = 2
    mock_api.files['remote_conflict.txt'] = b"server updated content"

    download_checked = False

    async def fake_handle_conflict(workspace, rel_path, server_file, conflict_file, on_resolve=None, runner=None):
        nonlocal download_checked
        assert server_file.read_text() == "server updated content"
        db_file = db.get_file(rel_path)
        assert db_file["server_revision"] == 2
        download_checked = True
        await asyncio.sleep(0.05)

    with patch("client.file_engine.handle_conflict_notification", side_effect=fake_handle_conflict):
        await engine.reconcile()
        assert len(engine._tasks) == 1
        await asyncio.gather(*engine._tasks)
        assert len(engine._tasks) == 0

    assert download_checked is True


@pytest.mark.asyncio
async def test_file_engine_reconcile_conflict_download_failure_no_notification(temp_workspace, mock_api):
    from unittest.mock import patch
    from client.config import ServerConfig

    local_path = temp_workspace / "fail_download.txt"
    local_path.write_text("initial")
    f_conf = FileConfig(local_path=local_path, remote_path="remote_fail.txt")

    db_path = temp_workspace / "db.sqlite3"
    db = LocalDB(db_path)
    engine = FileGroupEngine("test_ws", [f_conf], db, mock_api, ServerConfig())
    await engine.initial_sync()

    local_path.write_text("local edits")
    mock_api.remote_state['files'] = [{'path': 'remote_fail.txt', 'revision': 2, 'sha256': '999'}]
    mock_api.remote_state['latest_revision'] = 2

    async def failing_download(path):
        raise ConnectionError("Download error")

    mock_api.download_file = failing_download

    with patch("client.file_engine.handle_conflict_notification") as mock_notify:
        await engine.reconcile()
        mock_notify.assert_not_called()
        assert len(engine._tasks) == 0


@pytest.mark.asyncio
async def test_file_engine_reconcile_conflict_resolution_callback_holds_lock(temp_workspace, mock_api):
    import asyncio
    from unittest.mock import patch
    from client.config import ServerConfig

    local_path = temp_workspace / "lock_test.txt"
    local_path.write_text("initial")
    f_conf = FileConfig(local_path=local_path, remote_path="remote_lock.txt")

    db_path = temp_workspace / "db.sqlite3"
    db = LocalDB(db_path)
    engine = FileGroupEngine("test_ws", [f_conf], db, mock_api, ServerConfig())
    await engine.initial_sync()

    local_path.write_text("local edits")
    mock_api.remote_state['files'] = [{'path': 'remote_lock.txt', 'revision': 2, 'sha256': hashlib.sha256(b"server content").hexdigest()}]
    mock_api.remote_state['latest_revision'] = 2
    mock_api.files['remote_lock.txt'] = b"server content"

    captured_cb = None

    async def fake_handle_conflict(workspace, rel_path, server_file, conflict_file, on_resolve=None, runner=None):
        nonlocal captured_cb
        captured_cb = on_resolve

    with patch("client.file_engine.handle_conflict_notification", side_effect=fake_handle_conflict):
        await engine.reconcile()
        await asyncio.gather(*engine._tasks)

    assert captured_cb is not None

    lock_held_during_upload = False
    orig_upload = mock_api.upload_file

    async def fake_upload(path, file_path, **kwargs):
        nonlocal lock_held_during_upload
        lock_held_during_upload = engine._lock.locked()
        return await orig_upload(path, file_path, **kwargs)

    mock_api.upload_file = fake_upload
    await captured_cb("remote_lock.txt", "local")

    assert lock_held_during_upload is True
    db_file = db.get_file("remote_lock.txt")
    assert db_file["server_revision"] == 1

