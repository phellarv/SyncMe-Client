import pytest
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from client.engine import SyncEngine
from client.config import DirectoryConfig

@pytest.mark.asyncio
async def test_replace_symlinks_false_skips_local(tmp_path):
    db_mock = MagicMock()
    api_mock = AsyncMock()
    api_mock.get_state.return_value = {'files': [], 'last_revision': 0}
    api_mock.upload_file.return_value = {'revision': 1}
    engine = SyncEngine(tmp_path, db_mock, api_mock, replace_symlinks=False)
    
    # Create a local symlink
    target = tmp_path / "target.txt"
    target.write_text("hello")
    link = tmp_path / "link.txt"
    link.symlink_to("target.txt")
    
    db_mock.get_file.return_value = None
    db_mock.get_all_files.return_value = []
    
    await engine.reconcile()
    
    # Should not upload link because it's a symlink and replace_symlinks is False
    api_mock.upload_file.assert_called_once_with("target.txt", b"hello", expected_revision=0)

@pytest.mark.asyncio
async def test_replace_symlinks_true_uploads_local(tmp_path):
    db_mock = MagicMock()
    api_mock = AsyncMock()
    api_mock.get_state.return_value = {'files': [], 'last_revision': 0}
    api_mock.upload_file.return_value = {'revision': 1}
    engine = SyncEngine(tmp_path, db_mock, api_mock, replace_symlinks=True)
    
    # Create a local symlink
    target = tmp_path / "target.txt"
    target.write_text("hello")
    link = tmp_path / "link.txt"
    link.symlink_to("target.txt")
    
    db_mock.get_file.return_value = None
    db_mock.get_all_files.return_value = []
    
    await engine.reconcile()
    
    # Target and Link should be uploaded
    api_mock.upload_file.assert_any_call("target.txt", b"hello", expected_revision=0)
    api_mock.upload_file.assert_any_call("link.txt", b"hello", expected_revision=0)

@pytest.mark.asyncio
async def test_broken_symlink_does_not_crash(tmp_path):
    db_mock = MagicMock()
    api_mock = AsyncMock()
    api_mock.get_state.return_value = {'files': [], 'last_revision': 0}
    engine = SyncEngine(tmp_path, db_mock, api_mock, replace_symlinks=True)
    
    link = tmp_path / "link.txt"
    link.symlink_to("nonexistent.txt")
    
    db_mock.get_file.return_value = None
    db_mock.get_all_files.return_value = []
    
    await engine.reconcile()
    
    api_mock.upload_file.assert_not_called()

@pytest.mark.asyncio
async def test_replace_symlinks_false_preserves_local_on_download(tmp_path):
    db_mock = MagicMock()
    api_mock = AsyncMock()
    
    api_mock.get_state.return_value = {
        'files': [{'path': 'link.txt', 'revision': 2, 'is_deleted': False}],
        'last_revision': 2
    }
    api_mock.download_file.return_value = b"remote content"
    
    engine = SyncEngine(tmp_path, db_mock, api_mock, replace_symlinks=False)
    
    # Create a local symlink
    target = tmp_path / "target.txt"
    target.write_text("hello")
    link = tmp_path / "link.txt"
    link.symlink_to("target.txt")
    
    db_mock.is_initialized.return_value = True
    db_mock.get_last_server_revision.return_value = 1
    db_mock.get_file.return_value = None
    db_mock.get_all_files.return_value = []
    
    await engine.reconcile()
    
    # It should skip downloading over the symlink
    api_mock.download_file.assert_not_called()
    assert link.is_symlink()

@pytest.mark.asyncio
async def test_replace_symlinks_true_replaces_local(tmp_path):
    db_mock = MagicMock()
    api_mock = AsyncMock()
    
    api_mock.get_state.return_value = {
        'files': [{'path': 'link.txt', 'revision': 2, 'is_deleted': False}],
        'last_revision': 2
    }
    api_mock.download_file.return_value = b"remote content"
    
    engine = SyncEngine(tmp_path, db_mock, api_mock, replace_symlinks=True)
    
    # Create a local symlink
    target = tmp_path / "target.txt"
    target.write_text("hello")
    link = tmp_path / "link.txt"
    link.symlink_to("target.txt")
    
    db_mock.is_initialized.return_value = True
    db_mock.get_last_server_revision.return_value = 1
    db_mock.get_file.return_value = None
    db_mock.get_all_files.return_value = []
    
    await engine.reconcile()
    
    api_mock.download_file.assert_called_with("link.txt")
    assert not link.is_symlink()
    assert link.read_text() == "remote content"

def test_config_parsing():
    conf = DirectoryConfig(replace_symlinks=True)
    assert conf.replace_symlinks is True
