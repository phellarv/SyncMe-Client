"""Notification text escaping, config permission warning and client_id validation."""
import os
from pathlib import Path

import pytest
from click.testing import CliRunner

import client.cli as cli_module
from client.config import ClientConfig, ConfigError, ServerConfig, check_server_config, load_config, warn_if_config_loose
from client.notifications import handle_conflict_notification

GOOD_KEY = "k" * 43
CONFIG_SECRET = "CONFIGSECRET"

# --- notifications ----------------------------------------------------------------------------

ACTION_ARGS = [
    "-A", "meld=🔍 Resolve with Meld",
    "-A", "local=💾 Keep my local version",
    "-A", "server=☁️ Keep server version",
]


async def notify(tmp_path, *, workspace="ws", rel_path="doc.txt", name="doc.txt", action=""):
    """Run the handler with a fake runner; returns (notify-send cmd, every cmd that was run)."""
    server_file = tmp_path / name
    conflict_file = tmp_path / "doc.conflict.1.txt"
    cmds = []

    def runner(cmd):
        cmds.append(list(cmd))
        return (0, action if cmd[0] == "notify-send" else "")

    await handle_conflict_notification(
        workspace=workspace, rel_path=rel_path, server_file=server_file, conflict_file=conflict_file, runner=runner
    )
    return cmds[0], cmds


def title_and_body(cmd):
    return cmd[7], cmd[8]


def only_entities(text):
    """Every ampersand starts one of the three entities that escaping produces."""
    return text.count("&") == text.count("&amp;") + text.count("&lt;") + text.count("&gt;")


@pytest.mark.asyncio
async def test_markup_in_the_path_is_escaped_in_the_body(tmp_path):
    cmd, _ = await notify(tmp_path, rel_path='<b>x</b>&amp;<span foreground="red">')
    title, body = title_and_body(cmd)
    assert '&lt;b&gt;x&lt;/b&gt;&amp;amp;&lt;span foreground="red"&gt;' in body
    assert "<" not in body and ">" not in body and only_entities(body)


@pytest.mark.asyncio
async def test_markup_in_the_workspace_name_is_escaped(tmp_path):
    cmd, _ = await notify(tmp_path, workspace="<i>ws</i>&")
    _, body = title_and_body(cmd)
    assert "'&lt;i&gt;ws&lt;/i&gt;&amp;'" in body
    assert "<" not in body and ">" not in body and only_entities(body)


@pytest.mark.asyncio
async def test_markup_in_the_file_name_is_escaped_in_the_title(tmp_path):
    cmd, _ = await notify(tmp_path, name="<b>big<i>&.txt")  # a file name cannot contain a slash
    title, _ = title_and_body(cmd)
    assert title == "SyncMe Conflict: &lt;b&gt;big&lt;i&gt;&amp;.txt"


@pytest.mark.asyncio
async def test_long_path_is_truncated_to_120_characters_plus_an_ellipsis(tmp_path):
    cmd, _ = await notify(tmp_path, rel_path="x" * 5000)
    _, body = title_and_body(cmd)
    assert "x" * 120 + "…" in body
    assert "x" * 121 not in body
    assert len(body) < 300


@pytest.mark.asyncio
async def test_a_path_of_exactly_120_characters_is_not_truncated(tmp_path):
    cmd, _ = await notify(tmp_path, rel_path="x" * 120)
    _, body = title_and_body(cmd)
    assert "x" * 120 in body and "…" not in body


@pytest.mark.asyncio
async def test_long_file_name_is_truncated_in_the_title(tmp_path):
    cmd, _ = await notify(tmp_path, name="n" * 200)
    title, _ = title_and_body(cmd)
    assert title == "SyncMe Conflict: " + "n" * 120 + "…"


@pytest.mark.asyncio
async def test_long_workspace_name_is_truncated(tmp_path):
    cmd, _ = await notify(tmp_path, workspace="w" * 500)
    _, body = title_and_body(cmd)
    assert "w" * 120 + "…" in body and "w" * 121 not in body


@pytest.mark.asyncio
async def test_action_arguments_are_unchanged(tmp_path):
    cmd, _ = await notify(tmp_path, rel_path="<x>")
    assert cmd[9:] == ACTION_ARGS
    assert cmd[:7] == ["notify-send", "-u", "critical", "-a", "SyncMe", "-i", "dialog-warning"]


@pytest.mark.asyncio
async def test_paths_passed_to_meld_stay_unmodified(tmp_path):
    name = "<b>" + "n" * 200 + "&.txt"
    _, cmds = await notify(tmp_path, name=name, action="meld")
    meld = [c for c in cmds if c[0] == "meld"]
    assert meld == [["meld", str(tmp_path / name), str(tmp_path / "doc.conflict.1.txt")]]


# --- config permission warning ----------------------------------------------------------------


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    """A config file with a secret in it; the CLI runs with an incomplete [server] section (no key)."""
    path = tmp_path / "config.toml"
    path.write_text(f'[server]\napi_key = "{CONFIG_SECRET}"\n')
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(path))
    cfg = ClientConfig(server=ServerConfig())  # empty key: the command stops after the checks, nothing goes to the network
    cfg.directories[0].local_path = tmp_path / "sync"
    monkeypatch.setattr(cli_module, "settings", cfg)
    return path


@pytest.mark.parametrize("cmd", ["init", "run", "sync"])
def test_loose_config_permissions_warn_once_and_never_print_the_content(config_file, cmd):
    os.chmod(config_file, 0o644)
    result = CliRunner().invoke(cli_module.cli, [cmd])
    assert result.output.count("Warning:") == 1
    assert str(config_file) in result.output and f"chmod 600 {config_file}" in result.output
    assert "0o644" in result.output
    assert CONFIG_SECRET not in result.output


@pytest.mark.parametrize("mode", [0o600, 0o400])
def test_private_config_gives_no_warning(config_file, mode):
    os.chmod(config_file, mode)
    result = CliRunner().invoke(cli_module.cli, ["sync"])
    assert "Warning" not in result.output


@pytest.mark.parametrize("mode", [0o640, 0o604, 0o660, 0o606])
def test_group_or_other_access_of_any_kind_warns(config_file, mode):
    os.chmod(config_file, mode)
    assert "Warning:" in CliRunner().invoke(cli_module.cli, ["sync"]).output


def test_missing_config_file_gives_no_warning(config_file):
    config_file.unlink()
    assert "Warning" not in CliRunner().invoke(cli_module.cli, ["sync"]).output


def test_status_does_not_warn_because_it_is_local(config_file):
    os.chmod(config_file, 0o644)
    result = CliRunner().invoke(cli_module.cli, ["status"])
    assert result.exit_code == 0 and "Warning" not in result.output


def test_the_warning_goes_to_stderr(tmp_path, capsys):
    path = tmp_path / "c.toml"
    path.write_text("")
    os.chmod(path, 0o666)
    warn_if_config_loose(path)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert f"Warning: config file {path} is accessible by group or others (mode 0o666); run: chmod 600 {path}" in captured.err


def test_warn_if_config_loose_uses_the_configured_path_by_default(tmp_path, monkeypatch, capsys):
    path = tmp_path / "c.toml"
    path.write_text("")
    os.chmod(path, 0o644)
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(path))
    warn_if_config_loose()
    assert str(path) in capsys.readouterr().err


def test_a_symlinked_config_is_judged_by_its_target_not_by_the_link(tmp_path, capsys):
    """os.stat follows the link: a symlink itself always reports mode 0o777."""
    target = tmp_path / "dotfiles-config.toml"
    target.write_text("")
    os.chmod(target, 0o644)
    link = tmp_path / "config.toml"
    link.symlink_to(target)
    warn_if_config_loose(link)
    err = capsys.readouterr().err
    assert f"config file {link} is accessible by group or others (mode 0o644)" in err


def test_a_symlink_to_a_private_config_gives_no_warning(tmp_path, capsys):
    target = tmp_path / "dotfiles-config.toml"
    target.write_text("")
    os.chmod(target, 0o600)
    link = tmp_path / "config.toml"
    link.symlink_to(target)
    warn_if_config_loose(link)
    captured = capsys.readouterr()
    assert captured.err == "" and captured.out == ""


def test_a_dangling_symlink_gives_no_warning_and_does_not_crash(tmp_path, capsys):
    link = tmp_path / "config.toml"
    link.symlink_to(tmp_path / "missing.toml")
    warn_if_config_loose(link)  # nothing to warn about, and no exception
    captured = capsys.readouterr()
    assert captured.err == "" and captured.out == ""


# --- client_id --------------------------------------------------------------------------------


def srv(client_id):
    return ServerConfig(url="https://sync.example.org", api_key=GOOD_KEY, client_id=client_id)


@pytest.mark.parametrize("client_id", ["laptop-1", "a.b_c", "default_client", "X", "x" * 64])
def test_valid_client_ids_are_accepted(client_id):
    check_server_config(srv(client_id))


@pytest.mark.parametrize("client_id", ["bad\nSECRETID", "SECRETID\n", "bad SECRETID", "bäd-SECRETID", "", "x" * 65, "a/b", "a\tb"])
def test_invalid_client_ids_are_rejected_without_echoing_the_value(client_id):
    with pytest.raises(ConfigError, match="client_id") as exc:
        check_server_config(srv(client_id))
    assert "SECRETID" not in str(exc.value)
    assert client_id not in str(exc.value) or client_id == ""
    assert "1-64" in str(exc.value)


def test_client_id_from_the_environment_is_validated_too(tmp_path, monkeypatch):
    monkeypatch.setenv("SYNCME_CLIENT_ID", "bad id\nSECRETID")
    cfg = load_config(tmp_path / "absent.toml")
    with pytest.raises(ConfigError, match="client_id") as exc:
        check_server_config(ServerConfig(url="https://sync.example.org", api_key=GOOD_KEY, client_id=cfg.server.client_id))
    assert "SECRETID" not in str(exc.value)


def test_a_valid_client_id_from_the_environment_passes(tmp_path, monkeypatch):
    monkeypatch.setenv("SYNCME_CLIENT_ID", "laptop-2")
    cfg = load_config(tmp_path / "absent.toml")
    check_server_config(ServerConfig(url="https://sync.example.org", api_key=GOOD_KEY, client_id=cfg.server.client_id))


@pytest.mark.parametrize("cmd", [["init"], ["run"], ["sync"]])
def test_server_commands_refuse_a_bad_client_id(tmp_path, monkeypatch, cmd):
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(tmp_path / "config.toml"))
    cfg = ClientConfig(server=srv("bad id"))
    cfg.directories[0].local_path = tmp_path / "sync"
    monkeypatch.setattr(cli_module, "settings", cfg)
    result = CliRunner().invoke(cli_module.cli, cmd)
    assert result.exit_code != 0 and "client_id" in result.output
