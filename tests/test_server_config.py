import os
import re
import stat
import tomllib
from pathlib import Path

import pytest
from click.testing import CliRunner

import client.cli as cli_module
import client.config as config_module
from client.config import ClientConfig, ConfigError, ServerConfig, check_server_config, save_default_config

GOOD_KEY = "k" * 43


def srv(url="https://sync.example.org", key=GOOD_KEY):
    return ServerConfig(url=url, api_key=key)


def test_no_default_api_key():
    assert ServerConfig().api_key == ""


@pytest.mark.parametrize("key", ["", "   ", "default_token", "  default_token  "])
def test_missing_or_legacy_key_is_rejected(key):
    with pytest.raises(ConfigError, match="API key"):
        check_server_config(srv(key=key))


@pytest.mark.parametrize(
    "url",
    [
        "https://sync.example.org",
        "https://sync.example.org:8443/base",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "http://[::1]:8000",
    ],
)
def test_accepted_urls(url):
    check_server_config(srv(url=url))


@pytest.mark.parametrize(
    "url",
    [
        "http://sync.example.org",
        "http://10.0.0.5:8000",
        "http://localhost.evil.com",
        "http://127.0.0.1.evil.com",
        "http://evil.com/localhost",
        "http://localhost@evil.com",
        "ftp://localhost",
        "sync.example.org",
        "",
        "https://",
        "https:///path",
        "http://[::1",
    ],
)
def test_rejected_urls(url):
    with pytest.raises(ConfigError, match="https"):
        check_server_config(srv(url=url))


def test_default_config_has_empty_key_and_mode_0600(tmp_path):
    p = tmp_path / "SyncMe" / "config.toml"
    save_default_config(p)
    assert stat.S_IMODE(os.stat(p).st_mode) == 0o600
    text = p.read_text()
    assert 'api_key = ""' in text and "default_token" not in text


def test_existing_config_is_not_overwritten(tmp_path):
    p = tmp_path / "config.toml"
    p.write_text("keep me")
    save_default_config(p)
    assert p.read_text() == "keep me"


@pytest.fixture
def runner_env(tmp_path, monkeypatch):
    monkeypatch.setenv("SYNCME_CONFIG_PATH", str(tmp_path / "config.toml"))

    def use(server):
        cfg = ClientConfig(server=server)
        cfg.directories[0].local_path = tmp_path / "sync"
        monkeypatch.setattr(cli_module, "settings", cfg)

    return use


@pytest.fixture
def no_api_client(monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("the server config check must stop the command before any API client exists")

    monkeypatch.setattr(cli_module, "APIClient", boom)


@pytest.mark.parametrize("cmd", [["init"], ["run"], ["sync"]])
def test_server_commands_refuse_without_key(runner_env, no_api_client, cmd):
    runner_env(srv(key=""))
    result = CliRunner().invoke(cli_module.cli, cmd)
    assert result.exit_code != 0
    assert "API key" in result.output


@pytest.mark.parametrize("cmd", [["init"], ["run"], ["sync"]])
def test_server_commands_refuse_legacy_default_key(runner_env, no_api_client, cmd):
    runner_env(srv(key="default_token"))
    result = CliRunner().invoke(cli_module.cli, cmd)
    assert result.exit_code != 0
    assert "API key" in result.output


@pytest.mark.parametrize("cmd", [["init"], ["run"], ["sync"]])
def test_server_commands_refuse_plain_http(runner_env, no_api_client, cmd):
    runner_env(srv(url="http://sync.example.org"))
    result = CliRunner().invoke(cli_module.cli, cmd)
    assert result.exit_code != 0
    assert "https" in result.output


def test_init_without_key_still_creates_the_config_file_to_edit(runner_env, no_api_client, tmp_path):
    runner_env(srv(key=""))
    result = CliRunner().invoke(cli_module.cli, ["init"])
    assert result.exit_code != 0
    config = tmp_path / "config.toml"
    assert config.exists()
    assert stat.S_IMODE(os.stat(config).st_mode) == 0o600


def test_local_commands_do_not_need_a_key(runner_env):
    runner_env(srv(key=""))
    assert CliRunner().invoke(cli_module.cli, ["status"]).exit_code == 0


# --- API key shape: printable ASCII only, no whitespace, no stripping -------------------

KEY_BODY = "SECRETBODY"


@pytest.mark.parametrize(
    "key",
    [
        KEY_BODY + "\n",
        KEY_BODY + "\r",
        KEY_BODY + "\r\n",
        "SECRET\nBODY",
        "SECRET\rBODY",
        "SECRET\tBODY",
        "SECRET BODY",
        "  " + KEY_BODY + "  ",
        " " + KEY_BODY,
        KEY_BODY + " ",
        "\t" + KEY_BODY,
        "SECRET\x00BODY",
        "SECRET\x7fBODY",
        "SECRET\u00e9BODY",
        "SECRET\u00a0BODY",
        "SECRET\u200bBODY",
        "SECRET\u2028BODY",
        KEY_BODY + "\u00e5\u00f8",
    ],
    ids=repr,
)
def test_key_with_whitespace_control_or_non_ascii_is_rejected_without_echo(key):
    with pytest.raises(ConfigError, match="API key") as exc:
        check_server_config(srv(key=key))
    message = str(exc.value)
    assert KEY_BODY not in message and "SECRET" not in message
    assert repr(key) not in message and key not in message


def test_padded_key_is_rejected_not_stripped():
    with pytest.raises(ConfigError, match="API key"):
        check_server_config(srv(key="  abc  "))


@pytest.mark.parametrize(
    "key",
    [
        "k" * 5000,
        "!",
        "~",
        "a.b-c_d~e+f/g=h",
        "".join(chr(c) for c in range(0x21, 0x7F)),
    ],
    ids=["very-long", "bang", "tilde", "typical-token-chars", "all-printable-ascii"],
)
def test_printable_ascii_keys_are_accepted(key):
    check_server_config(srv(key=key))


@pytest.mark.parametrize("cmd", [["init"], ["run"], ["sync"]])
def test_server_commands_refuse_malformed_key_without_echo(runner_env, no_api_client, cmd):
    runner_env(srv(key=KEY_BODY + "\n"))
    result = CliRunner().invoke(cli_module.cli, cmd)
    assert result.exit_code != 0
    assert isinstance(result.exception, SystemExit)
    assert "API key" in result.output
    assert "SECRET" not in result.output


# --- URL checks: credentials, ports, hosts, httpx -----------------------------------------

PW = "pw-not-real"
USERINFO_URLS = [
    "https://" + "user:" + PW + "@sync.example.org",
    "https://" + "user@sync.example.org",
    "https://" + "@sync.example.org",
    "https://" + ":" + PW + "@sync.example.org",
    "http://" + "user:" + PW + "@localhost:8000",
    "http://" + "user:" + PW + "@127.0.0.1:8000",
    "http://" + "user:" + PW + "@[::1]:8000",
]


@pytest.mark.parametrize("url", USERINFO_URLS)
def test_url_with_userinfo_is_rejected(url):
    with pytest.raises(ConfigError, match="user name or password"):
        check_server_config(srv(url=url))


def test_bad_port_is_a_config_error():
    for url in ["https://example.org:99999", "https://example.org:abc", "https://example.org:-1"]:
        with pytest.raises(ConfigError, match="not a valid"):
            check_server_config(srv(url=url))


@pytest.mark.parametrize("url", ["https://example.org:99999", "https://example.org:65536"])
def test_out_of_range_port_is_rejected(url):
    # httpx.URL() itself accepts these, so only an explicit port check catches them
    with pytest.raises(ConfigError, match="not a valid"):
        check_server_config(srv(url=url))


@pytest.mark.parametrize("url", ["https://exa mple.org", "https://exa\u00a0mple.org", "https://example.org .evil.com"])
def test_whitespace_in_host_is_rejected(url):
    # httpx.URL() accepts a space in the host, so only an explicit host check catches it
    with pytest.raises(ConfigError, match="not a valid"):
        check_server_config(srv(url=url))


@pytest.mark.parametrize(
    "url",
    [
        "http://local\nhost:8000",  # urlparse strips the newline and sees localhost
        "https://example.org/\x00",
        "https://example.org/\t",
        "https://example.org/a\x01b",
    ],
    ids=repr,
)
def test_urls_httpx_would_reject_are_config_errors(url):
    # urlparse accepts all of these; httpx.URL() raises InvalidURL on them
    with pytest.raises(ConfigError, match="not a valid"):
        check_server_config(srv(url=url))


def test_unbalanced_ipv6_bracket_is_a_config_error():
    # already caught by the urlparse ValueError path, not by the httpx check
    with pytest.raises(ConfigError, match="not a valid"):
        check_server_config(srv(url="https://[::1"))


# Every rejection branch has its own message; none may echo a password or other secret.
SECRET_URLS = [
    ("userinfo on a valid https host", "https://" + "user:" + PW + "@sync.example.org"),
    ("userinfo, empty user name", "https://" + ":" + PW + "@sync.example.org"),
    ("userinfo on localhost", "http://" + "user:" + PW + "@localhost:8000"),
    ("plain http with userinfo", "http://" + "user:" + PW + "@sync.example.org"),
    ("bad scheme with userinfo", "ftp://" + "user:" + PW + "@sync.example.org"),
    ("bad port", "https://" + "user:" + PW + "@example.org:99999"),
    ("password in port position", "https://" + "user:" + PW),
    ("space in host", "https://" + "user:" + PW + "@exa mple.org"),
    ("httpx-only reject", "https://" + "user:" + PW + "@example.org/\x00"),
    ("broken bracket", "https://" + "user:" + PW + "@[::1"),
    ("no scheme", "user:" + PW + "@sync.example.org"),
    ("no host", "https://" + "user:" + PW + "@"),
    ("password before a fragment", "http://" + PW + "#frag@sync.example.org"),
    ("secret in the query", "http://sync.example.org/path?password=" + PW),
    ("secret in the path", "http://sync.example.org/" + PW),
    ("secret in the fragment", "http://sync.example.org/#" + PW),
]


@pytest.mark.parametrize("url", [u for _, u in SECRET_URLS], ids=[n for n, _ in SECRET_URLS])
def test_no_error_message_echoes_a_secret_from_the_url(url):
    with pytest.raises(ConfigError) as exc:
        check_server_config(srv(url=url))
    assert PW not in str(exc.value)
    assert PW not in repr(exc.value)


@pytest.mark.parametrize(
    "url, shown",
    [
        ("http://sync.example.org:8000/base?x=y", "http://sync.example.org:8000"),
        ("http://sync.example.org/a/b", "http://sync.example.org"),
        ("ftp://sync.example.org:21", "ftp://sync.example.org:21"),
        ("http://[2001:db8::1]:8000/x", "http://[2001:db8::1]:8000"),
    ],
)
def test_error_echoes_only_scheme_host_and_port(url, shown):
    with pytest.raises(ConfigError, match="must use https") as exc:
        check_server_config(srv(url=url))
    message = str(exc.value)
    assert shown in message
    for leaked in ("/base", "/a/b", "x=y", "/x"):
        assert leaked not in message.replace("https://", "").replace("http://", "")


# --- save_default_config: races and dangling symlinks -------------------------------------


def test_dangling_symlink_at_config_path_is_left_alone(tmp_path):
    target = tmp_path / "nowhere" / "config.toml"
    p = tmp_path / "config.toml"
    p.symlink_to(target)
    save_default_config(p)  # must not raise
    assert p.is_symlink()
    assert not target.exists() and not target.parent.exists()


def test_file_created_between_exists_check_and_open_is_not_overwritten(tmp_path, monkeypatch):
    p = tmp_path / "config.toml"
    real_open = os.open

    def racing_open(path, flags, mode=0o777, **kwargs):
        if Path(path) == p:
            p.write_text("keep me")  # another process wins the race
        return real_open(path, flags, mode, **kwargs)

    monkeypatch.setattr(config_module.os, "open", racing_open)
    save_default_config(p)  # must not raise
    assert p.read_text() == "keep me"


# --- docs: the documented example must not be a working key -------------------------------

ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize("doc", ["README.md", "TLDR.md"])
def test_documented_example_config_fails_the_check_until_a_key_is_pasted(doc):
    text = (ROOT / doc).read_text()
    blocks = [b for b in re.findall(r"```toml\n(.*?)```", text, re.S) if "api_key" in b]
    assert blocks, f"{doc} has no toml example with api_key"
    for block in blocks:
        server = ServerConfig(**tomllib.loads(block)["server"])
        with pytest.raises(ConfigError, match="No API key configured"):
            check_server_config(server)
