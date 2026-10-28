import json
from pathlib import Path
from click.testing import CliRunner
from client.cli import cli


def test_cli_targets_human_readable(tmp_path: Path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))

    existing_dir = tmp_path / "existing_dir"
    existing_dir.mkdir()
    missing_dir = tmp_path / "missing_dir"

    existing_file = tmp_path / "existing_file.txt"
    existing_file.write_text("hello")
    missing_file = tmp_path / "missing_file.txt"

    toml_content = f"""[server]
url = "http://localhost:8000"
api_key = "super-secret-api-key"

[[directories]]
name = "workspace_dir1"
local_path = "{existing_dir}"
conflict_strategy = "ask"
exceptions = ["*.log", ".tmp"]

[[directories]]
name = "workspace_dir2"
local_path = "{missing_dir}"
conflict_strategy = "server"

[[files]]
workspace = "workspace_files"
local_path = "{existing_file}"
conflict_strategy = "local"

[[files]]
workspace = "workspace_files"
local_path = "{missing_file}"
remote_path = "custom_missing.txt"
"""
    config_file.write_text(toml_content)

    runner = CliRunner()
    result = runner.invoke(cli, ["targets"])

    assert result.exit_code == 0
    output = result.output

    # Verify no secrets leaked
    assert "super-secret-api-key" not in output

    # Verify directories
    assert "workspace_dir1" in output
    assert str(existing_dir) in output
    assert "exists" in output
    assert "*.log, .tmp" in output

    assert "workspace_dir2" in output
    assert str(missing_dir) in output
    assert "does not exist" in output

    # Verify files
    assert "workspace_files" in output
    assert str(existing_file) in output
    assert str(missing_file) in output
    assert "custom_missing.txt" in output


def test_cli_targets_json(tmp_path: Path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))

    existing_dir = tmp_path / "my_dir"
    existing_dir.mkdir()
    existing_file = tmp_path / "my_file.txt"
    existing_file.write_text("data")

    toml_content = f"""[server]
url = "http://localhost:8000"
api_key = "super-secret-api-key"

[[directories]]
name = "my_dir"
local_path = "{existing_dir}"
conflict_strategy = "ask"

[[files]]
workspace = "my_files"
local_path = "{existing_file}"
remote_path = "remote_my_file.txt"
"""
    config_file.write_text(toml_content)

    runner = CliRunner()
    result = runner.invoke(cli, ["targets", "--json"])

    assert result.exit_code == 0
    assert "super-secret-api-key" not in result.output

    data = json.loads(result.output)
    assert "directories" in data
    assert "files" in data

    # Check directories
    assert len(data["directories"]) == 1
    d = data["directories"][0]
    assert d["name"] == "my_dir"
    assert d["local_path"] == str(existing_dir)
    assert isinstance(d["local_path"], str)
    assert d["conflict_strategy"] == "ask"

    # Check files
    assert len(data["files"]) == 1
    f = data["files"][0]
    assert f["workspace"] == "my_files"
    assert f["local_path"] == str(existing_file)
    assert isinstance(f["local_path"], str)
    assert f["remote_path"] == "remote_my_file.txt"


def test_cli_targets_empty(tmp_path: Path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))

    toml_content = """[server]
url = "http://localhost:8000"
api_key = "test-key"
"""
    config_file.write_text(toml_content)

    runner = CliRunner()
    result = runner.invoke(cli, ["targets"])
    assert result.exit_code == 0
    assert "Files:\n  (none)" in result.output


def test_cli_targets_config_error(tmp_path: Path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))

    # Duplicate directory workspace names trigger ConfigError
    toml_content = """[server]
url = "http://localhost:8000"
api_key = "test-key"

[[directories]]
name = "dup"
local_path = "~/dir1"

[[directories]]
name = "dup"
local_path = "~/dir2"
"""
    config_file.write_text(toml_content)

    runner = CliRunner()
    result = runner.invoke(cli, ["targets"])
    assert result.exit_code == 1
    assert "Duplicate directory workspace name" in result.output


def test_cli_targets_inaccessible_path(tmp_path: Path, monkeypatch):
    from unittest.mock import patch
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))

    toml_content = """[server]
url = "http://localhost:8000"
api_key = "test-key"

[[directories]]
name = "test_dir"
local_path = "/root/secret_dir"
"""
    config_file.write_text(toml_content)

    orig_exists = Path.exists

    def selective_exists(self):
        if str(self) == "/root/secret_dir":
            raise PermissionError("Permission denied")
        return orig_exists(self)

    runner = CliRunner()
    with patch("pathlib.Path.exists", new=selective_exists):
        result = runner.invoke(cli, ["targets"])
        assert result.exit_code == 0
        assert "inaccessible" in result.output


def test_cli_targets_toml(tmp_path: Path, monkeypatch):
    import tomllib
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))

    existing_dir = tmp_path / "my_dir"
    existing_dir.mkdir()
    existing_file = tmp_path / "my_file.txt"
    existing_file.write_text("data")

    toml_content = f"""[server]
url = "http://localhost:8000"
api_key = "super-secret-api-key"

[[directories]]
name = "my_dir"
local_path = "{existing_dir}"
conflict_strategy = "server"
replace_symlinks = true
exceptions = ["cache", "*.log"]

[[files]]
workspace = "my_files"
local_path = "{existing_file}"
remote_path = "remote_my_file.txt"
conflict_strategy = "local"
replace_symlinks = false
"""
    config_file.write_text(toml_content)

    runner = CliRunner()
    result = runner.invoke(cli, ["targets", "--toml"])

    assert result.exit_code == 0
    assert "super-secret-api-key" not in result.output

    parsed = tomllib.loads(result.output)
    assert "directories" in parsed
    assert "files" in parsed
    assert len(parsed["directories"]) == 1
    assert parsed["directories"][0]["name"] == "my_dir"
    assert parsed["directories"][0]["conflict_strategy"] == "server"
    assert parsed["directories"][0]["replace_symlinks"] is True
    assert parsed["directories"][0]["exceptions"] == ["cache", "*.log"]

    assert len(parsed["files"]) == 1
    assert parsed["files"][0]["workspace"] == "my_files"
    assert parsed["files"][0]["remote_path"] == "remote_my_file.txt"
    assert parsed["files"][0]["conflict_strategy"] == "local"
    assert parsed["files"][0]["replace_symlinks"] is False


def test_cli_targets_json_and_toml_mutual_exclusion(tmp_path: Path, monkeypatch):
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(config_file))

    toml_content = """[server]
url = "http://localhost:8000"
api_key = "test-key"
"""
    config_file.write_text(toml_content)

    runner = CliRunner()
    result = runner.invoke(cli, ["targets", "--json", "--toml"])
    assert result.exit_code != 0
    assert "Cannot specify both --json and --toml" in result.output

