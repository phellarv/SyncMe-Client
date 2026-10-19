import os
import pytest
from pathlib import Path
from client.config import load_config, save_default_config

def test_load_default_config(tmp_path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))
    
    save_default_config()
    
    assert config_file.exists()
    
    config = load_config()
    
    assert config.server.url == "http://localhost:8000"
    assert len(config.directories) == 1
    assert config.directories[0].name == "default"
    assert config.directories[0].local_path == Path("~/.syncme_data").expanduser()

def test_env_var_overrides(tmp_path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))
    
    save_default_config()
    
    monkeypatch.setenv("SYNCME_SERVER_URL", "http://testserver")
    monkeypatch.setenv("SYNCME_SYNC_DIR", "~/test_sync")
    
    config = load_config()
    
    assert config.server.url == "http://testserver"
    assert config.directories[0].local_path == Path("~/test_sync").expanduser()

def test_expand_home_directory(tmp_path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))
    
    toml_content = """[server]
url = "http://localhost:8000"

[[directories]]
name = "work"
local_path = "~/work_sync"
"""
    config_file.write_text(toml_content)
    
    config = load_config()
    assert config.directories[0].local_path == Path("~/work_sync").expanduser()

def test_directory_exceptions_comma_separated(tmp_path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))
    
    toml_content = """[server]
url = "http://localhost:8000"

[[directories]]
name = "work"
local_path = "~/work_sync"
exceptions = "fil1,fil2"
"""
    config_file.write_text(toml_content)
    
    config = load_config()
    assert config.directories[0].exceptions == ["fil1", "fil2"]

def test_directory_exceptions_list_and_whitespace(tmp_path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))
    
    toml_content = """[server]
url = "http://localhost:8000"

[[directories]]
name = "work"
local_path = "~/work_sync"
exceptions = ["fil1", " fil2 "]
"""
    config_file.write_text(toml_content)
    
    config = load_config()
    assert config.directories[0].exceptions == ["fil1", "fil2"]

def test_directory_exceptions_default_empty(tmp_path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))
    save_default_config()
    
    config = load_config()
    assert config.directories[0].exceptions == []

def test_directory_exceptions_validation_errors(tmp_path, monkeypatch):
    from pydantic import ValidationError
    from client.config import DirectoryConfig

    # Exclamation mark negation is disallowed
    with pytest.raises(ValidationError):
        DirectoryConfig(exceptions=["!unignore"])

    # Non-strings in list are disallowed
    with pytest.raises(ValidationError):
        DirectoryConfig(exceptions=[123])


def test_dead_code_removed():
    from client.config import ClientConfig
    from client.api import APIClient
    assert "SYNC_DIR" not in ClientConfig.__dict__
    assert not hasattr(APIClient, "get_health")
