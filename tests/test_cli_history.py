import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from click.testing import CliRunner
import client.cli as cli_module
from client.cli import cli
from pathlib import Path
import os

@pytest.fixture
def mock_engines(tmp_path):
    with patch("client.cli._get_engines") as mock_get_engines:
        with patch("client.cli._require_server_config"):
            d_conf = MagicMock()
            d_conf.local_path = tmp_path
            d_conf.name = "default"
            
            engine = MagicMock()
            engine.api = MagicMock()
            engine.api.close = AsyncMock()
            engine.api.download_file = AsyncMock(return_value=b"restored content")
            
            mock_get_engines.return_value = [(d_conf, engine)]
            
            yield d_conf, engine

def test_cli_history(mock_engines, tmp_path):
    d_conf, engine = mock_engines
    
    # Create the target file in tmp_path so the CLI resolves it correctly
    test_file = tmp_path / "test.txt"
    test_file.touch()
    
    engine.api.get_history = AsyncMock(return_value={
        "history": [
            {"revision": 2, "created_at": 1700000000.0, "size": 150, "client_name": "client2", "is_deleted": False},
            {"revision": 1, "created_at": 1700000000.0, "size": 100, "client_name": "client1", "is_deleted": True}
        ]
    })
    
    # We pass the absolute path to test_file so Path().resolve() matches it against local_path
    runner = CliRunner()
    result = runner.invoke(cli, ["history", str(test_file)])
    assert result.exit_code == 0
    assert "Revision" in result.output
    assert "2" in result.output
    assert "150" in result.output
    assert "client2" in result.output
    assert "1" in result.output
    assert "Yes" in result.output
    engine.api.get_history.assert_called_once_with("test.txt", limit=50)

def test_cli_restore(mock_engines, tmp_path):
    d_conf, engine = mock_engines
    
    test_file = tmp_path / "test.txt"
    test_file.write_text("old content")
    
    engine.db.get_file.return_value = {"server_revision": 5}
    engine.api.restore_history = AsyncMock(return_value={"new_revision": 3})
    
    runner = CliRunner()
    result = runner.invoke(cli, ["restore", str(test_file), "--revision", "1"])
    assert result.exit_code == 0
    assert "Successfully restored" in result.output
    engine.db.get_file.assert_called_once_with("test.txt")
    engine.api.restore_history.assert_called_once_with("test.txt", 1, expected_revision=5)
    assert test_file.read_bytes() == b"restored content"
    engine.api.download_file.assert_called_once_with("test.txt")

def test_cli_restore_with_real_local_db(mock_engines, tmp_path):
    from client.db import LocalDB
    d_conf, engine = mock_engines
    
    test_file = tmp_path / "test.txt"
    test_file.touch()
    
    real_db = LocalDB(tmp_path / ".syncme" / "local_state.db")
    try:
        real_db.upsert_file("test.txt", "abc", 10, 1000.0, 5, False)
        engine.db = real_db
        engine.api.restore_history = AsyncMock(return_value={"new_revision": 3})
        
        runner = CliRunner()
        result = runner.invoke(cli, ["restore", str(test_file), "--revision", "1"])
        assert result.exit_code == 0
        assert "Successfully restored" in result.output
        engine.api.restore_history.assert_called_once_with("test.txt", 1, expected_revision=5)
    finally:
        real_db.conn.close()

def test_cli_restore_file_not_in_db(mock_engines, tmp_path):
    d_conf, engine = mock_engines
    
    test_file = tmp_path / "test.txt"
    test_file.touch()
    
    engine.db.get_file.return_value = None
    engine.api.restore_history = AsyncMock(return_value={"new_revision": 3})
    
    runner = CliRunner()
    result = runner.invoke(cli, ["restore", str(test_file), "--revision", "1"])
    assert result.exit_code == 0
    assert "Successfully restored" in result.output
    engine.db.get_file.assert_called_once_with("test.txt")
    engine.api.restore_history.assert_called_once_with("test.txt", 1, expected_revision=None)


def test_cli_history_standalone_file(tmp_path):
    """Criterion: syncme-client history works for files in [[files]] workspaces."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    standalone_file = tmp_path / "config" / "config.fish"
    standalone_file.parent.mkdir(parents=True)
    standalone_file.touch()

    with patch("client.cli._get_engines") as mock_get_engines, \
         patch("client.cli._require_server_config"):
        from client.file_engine import FileGroupEngine
        from client.config import FileConfig

        d_conf = MagicMock()
        d_conf.local_path = state_dir

        engine = MagicMock(spec=FileGroupEngine)
        engine.files = [
            FileConfig(local_path=standalone_file, workspace="standalone", remote_path="config.fish")
        ]
        engine.api = MagicMock()
        engine.api.close = AsyncMock()
        engine.api.get_history = AsyncMock(return_value={"history": []})

        mock_get_engines.return_value = [(d_conf, engine)]

        runner = CliRunner()
        result = runner.invoke(cli, ["history", str(standalone_file)])
        assert result.exit_code == 0
        engine.api.get_history.assert_called_once_with("config.fish", limit=50)


def test_cli_restore_standalone_file(tmp_path):
    """Criterion: syncme-client restore works for files in [[files]] workspaces."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    standalone_file = tmp_path / "config" / "config.fish"
    standalone_file.parent.mkdir(parents=True)
    standalone_file.touch()

    with patch("client.cli._get_engines") as mock_get_engines, \
         patch("client.cli._require_server_config"):
        from client.file_engine import FileGroupEngine
        from client.config import FileConfig

        d_conf = MagicMock()
        d_conf.local_path = state_dir

        engine = MagicMock(spec=FileGroupEngine)
        engine.files = [
            FileConfig(local_path=standalone_file, workspace="standalone", remote_path="config.fish")
        ]
        engine.db = MagicMock()
        engine.db.get_file.return_value = {"server_revision": 1}
        engine.api = MagicMock()
        engine.api.close = AsyncMock()
        engine.api.restore_history = AsyncMock(return_value={"new_revision": 2})
        engine.api.download_file = AsyncMock(return_value=b"standalone content")

        mock_get_engines.return_value = [(d_conf, engine)]

        runner = CliRunner()
        result = runner.invoke(cli, ["restore", str(standalone_file), "--revision", "1"])
        assert result.exit_code == 0
        assert "Successfully restored" in result.output
        assert standalone_file.read_bytes() == b"standalone content"
        engine.db.get_file.assert_called_once_with("config.fish")
        engine.api.restore_history.assert_called_once_with("config.fish", 1, expected_revision=1)
        engine.api.download_file.assert_called_once_with("config.fish")


def test_cli_history_unmatched_file(tmp_path):
    """Criterion: file not in any engine produces error message and exit code 1."""
    with patch("client.cli._get_engines") as mock_get_engines, \
         patch("client.cli._require_server_config"):
        d_conf = MagicMock()
        d_conf.local_path = tmp_path / "sync_dir"
        engine = MagicMock()
        mock_get_engines.return_value = [(d_conf, engine)]
        runner = CliRunner()
        result = runner.invoke(cli, ["history", str(tmp_path / "other" / "unknown.txt")])
        assert result.exit_code != 0


def test_cli_restore_hash_mismatch(mock_engines, tmp_path):
    """Criterion: restore verifies sha256 from restore_history response before writing local file."""
    d_conf, engine = mock_engines

    test_file = tmp_path / "test.txt"
    test_file.write_text("old content")

    engine.db.get_file.return_value = {"server_revision": 5}
    engine.api.restore_history = AsyncMock(return_value={
        "revision": 6,
        "sha256": "0" * 64,
    })
    engine.api.download_file = AsyncMock(return_value=b"tampered content")

    runner = CliRunner()
    result = runner.invoke(cli, ["restore", str(test_file), "--revision", "1"])
    assert result.exit_code != 0
    assert test_file.read_text() == "old content"
    engine.db.upsert_file.assert_not_called()
