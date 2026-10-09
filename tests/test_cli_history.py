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
    test_file.touch()
    
    engine.db.get_file.return_value = {"revision": 5}
    engine.api.restore_history = AsyncMock(return_value={"new_revision": 3})
    
    runner = CliRunner()
    result = runner.invoke(cli, ["restore", str(test_file), "--revision", "1"])
    assert result.exit_code == 0
    assert "Successfully restored" in result.output
    engine.api.restore_history.assert_called_once_with("test.txt", 1, expected_revision=5)
