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


def test_get_sync_targets_on_client_config(tmp_path):
    from client.config import ClientConfig, DirectoryConfig, FileConfig

    dir1 = DirectoryConfig(name="docs", local_path=tmp_path / "docs")
    file1 = FileConfig(local_path=tmp_path / "file.txt", workspace="notes")
    cfg = ClientConfig(directories=[dir1], files=[file1])

    targets = cfg.get_sync_targets()
    assert isinstance(targets, dict)
    assert "directories" in targets
    assert "files" in targets
    assert targets["directories"] == [dir1]
    assert targets["files"] == [file1]


def test_get_sync_targets_function(tmp_path, monkeypatch):
    from client.config import get_sync_targets, ClientConfig, DirectoryConfig, FileConfig

    # 1. With explicit config argument
    dir1 = DirectoryConfig(name="code", local_path=tmp_path / "code")
    file1 = FileConfig(local_path=tmp_path / "app.py", workspace="code_files")
    cfg = ClientConfig(directories=[dir1], files=[file1])

    targets = get_sync_targets(cfg)
    assert targets["directories"] == [dir1]
    assert targets["files"] == [file1]

    # 2. Default fallback to load_config()
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))
    default_targets = get_sync_targets()
    assert len(default_targets["directories"]) == 1
    assert default_targets["directories"][0].name == "default"
    assert default_targets["files"] == []


def test_get_sync_targets_toml_roundtrip(tmp_path):
    import tomllib
    from client.config import get_sync_targets_toml, ClientConfig, DirectoryConfig, FileConfig

    dir1 = DirectoryConfig(
        name="workspace1",
        local_path=tmp_path / "dir1",
        conflict_strategy="server",
        replace_symlinks=True,
        exceptions=["*.tmp", "cache"]
    )
    file1 = FileConfig(
        local_path=tmp_path / "notes.txt",
        workspace="notes_ws",
        remote_path="remote_notes.txt",
        replace_symlinks=True,
        conflict_strategy="local"
    )
    cfg = ClientConfig(directories=[dir1], files=[file1])

    toml_out = get_sync_targets_toml(cfg, portable=False)
    assert "[[directories]]" in toml_out
    assert "[[files]]" in toml_out
    assert "name = \"workspace1\"" in toml_out
    assert "workspace = \"notes_ws\"" in toml_out

    # Test roundtrip parsing
    parsed = tomllib.loads(toml_out)
    reloaded_cfg = ClientConfig(**parsed)
    assert len(reloaded_cfg.directories) == 1
    assert reloaded_cfg.directories[0].name == "workspace1"
    assert reloaded_cfg.directories[0].conflict_strategy == "server"
    assert reloaded_cfg.directories[0].replace_symlinks is True
    assert reloaded_cfg.directories[0].exceptions == ["*.tmp", "cache"]

    assert len(reloaded_cfg.files) == 1
    assert reloaded_cfg.files[0].workspace == "notes_ws"
    assert reloaded_cfg.files[0].remote_path == "remote_notes.txt"
    assert reloaded_cfg.files[0].conflict_strategy == "local"
    assert reloaded_cfg.files[0].replace_symlinks is True

    # Test method on ClientConfig
    assert cfg.get_sync_targets_toml(portable=False) == toml_out


def test_get_sync_targets_toml_portable_and_unicode(tmp_path, monkeypatch):
    import tomllib
    from client.config import get_sync_targets_toml, ClientConfig, DirectoryConfig, FileConfig

    monkeypatch.setenv("HOME", str(tmp_path))

    dir1 = DirectoryConfig(
        name="📁 Dokuménter",
        local_path=tmp_path / "Dokuménter",
        conflict_strategy="ask",
        replace_symlinks=False,
        exceptions=["fil1", "fil2"]
    )
    file1 = FileConfig(
        local_path=tmp_path / "Bøker" / "bok.pdf",
        workspace="files",
        remote_path="bok.pdf"
    )
    cfg = ClientConfig(directories=[dir1], files=[file1])

    toml_out = get_sync_targets_toml(cfg, portable=True)
    assert 'local_path = "~/Dokuménter"' in toml_out
    assert 'local_path = "~/Bøker/bok.pdf"' in toml_out
    assert 'name = "📁 Dokuménter"' in toml_out

    # Test roundtrip with tilde expansion
    parsed = tomllib.loads(toml_out)
    reloaded_cfg = ClientConfig(**parsed)
    assert reloaded_cfg.directories[0].local_path == tmp_path / "Dokuménter"
    assert reloaded_cfg.files[0].local_path == tmp_path / "Bøker" / "bok.pdf"
    assert reloaded_cfg.directories[0].name == "📁 Dokuménter"


