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
