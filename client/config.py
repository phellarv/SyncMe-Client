import os
import re
import sys
import json
import tomllib
import unicodedata
from pathlib import Path
from typing import Optional, List
from urllib.parse import urlparse
import httpx
from pydantic import BaseModel, Field, model_validator, field_validator

class ServerConfig(BaseModel):
    url: str = "http://localhost:8000"
    api_key: str = ""
    client_id: str = "default_client"
    debounce_seconds: float = 1.0
    poll_interval_seconds: int = 300
    request_timeout: float = 300.0

class ConfigError(Exception):
    pass


_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_REJECTED_KEYS = {"", "default_token"}
# A key is sent verbatim in `Authorization: Bearer <key>`: printable ASCII, no whitespace.
_KEY_RE = re.compile(r"[\x21-\x7e]+")
_BAD_HOST_CHARS = re.compile(r"[\s\x00-\x1f\x7f]")
# client_id is sent verbatim in the X-Client-ID header.
_CLIENT_ID_RE = re.compile(r"[A-Za-z0-9._-]{1,64}")

_NO_KEY_MESSAGE = (
    "No API key configured. Create one on the server with "
    "`docker compose run --rm keys add <name>` and set `api_key` in the [server] "
    "section of the config file (or SYNCME_API_KEY)."
)
_BAD_KEY_MESSAGE = (
    "The configured API key contains characters that are not allowed: it must consist of "
    "printable ASCII characters only, without spaces, tabs or line breaks. Check for stray "
    "whitespace or a trailing newline around the key in the config file (or SYNCME_API_KEY)."
)
_BAD_URL_MESSAGE = (
    "Server URL is not a valid https:// URL (check the scheme, host name and port, and look "
    "for stray spaces or control characters)."
)
_USERINFO_MESSAGE = (
    "Server URL must be an https:// URL without a user name or password in front of the host "
    "name. The key is sent as a Bearer token; set it in `api_key` instead."
)


def _safe_origin(url: str) -> Optional[str]:
    """scheme://hostname[:port] of the URL, or None. Never userinfo, path, query or fragment."""
    if "@" in url:  # may hold credentials: echo nothing rather than guess what is host and what is secret
        return None
    try:
        parsed = urlparse(url)
        host, port = parsed.hostname, parsed.port
    except ValueError:
        return None
    if not parsed.scheme or not host:
        return None
    if ":" in host:
        host = f"[{host}]"
    return f"{parsed.scheme}://{host}" + (f":{port}" if port is not None else "")


def _check_api_key(key: str) -> None:
    # The strip only picks the friendlier message for blank or legacy values; whether a key is
    # accepted never depends on it: the key is used exactly as written, so padding is rejected.
    if key.strip() in _REJECTED_KEYS:
        raise ConfigError(_NO_KEY_MESSAGE)
    if not _KEY_RE.fullmatch(key):  # fullmatch: `$` would let a trailing newline through
        raise ConfigError(_BAD_KEY_MESSAGE)  # never echo the key


def _check_url(url: str) -> None:
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        parsed.port  # raises ValueError for a non-numeric or out-of-range port (httpx.URL accepts 99999)
    except ValueError:  # e.g. an unbalanced IPv6 bracket
        raise ConfigError(_BAD_URL_MESSAGE) from None
    if "@" in parsed.netloc:
        raise ConfigError(_USERINFO_MESSAGE)
    try:
        httpx.URL(url)  # urlparse silently strips newlines and tabs; httpx rejects what it cannot send
    except (httpx.InvalidURL, ValueError):
        raise ConfigError(_BAD_URL_MESSAGE) from None
    if hostname and _BAD_HOST_CHARS.search(hostname):  # httpx.URL accepts 'exa mple.org'
        raise ConfigError(_BAD_URL_MESSAGE)
    # Every check above runs before any allow decision, so the localhost exception cannot skip them.
    if parsed.scheme == "https" and hostname:
        return
    if parsed.scheme == "http" and hostname in _LOCAL_HOSTS:
        return
    origin = _safe_origin(url)
    shown = f" {origin!r}" if origin else ""
    raise ConfigError(
        f"Server URL{shown} must use https:// (plain http:// is only allowed for localhost)."
    )


def _check_client_id(client_id: str) -> None:
    if not _CLIENT_ID_RE.fullmatch(client_id):  # fullmatch: `$` would let a trailing newline through
        raise ConfigError("client_id must be 1-64 characters from A-Z a-z 0-9 . _ -")  # never echo the value


def check_server_config(server: ServerConfig) -> None:
    _check_api_key(server.api_key)
    _check_url(server.url)
    _check_client_id(server.client_id)

class DirectoryConfig(BaseModel):
    name: str = "default"
    local_path: Path = Field(default_factory=lambda: Path("~/.syncme_data").expanduser())
    conflict_strategy: str = "ask"
    replace_symlinks: bool = False
    exceptions: List[str] = Field(default_factory=list)

    @field_validator("exceptions", mode="before")
    @classmethod
    def parse_exceptions(cls, v):
        if v is None:
            return []
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, list):
            res = []
            for item in v:
                if not isinstance(item, str):
                    raise ValueError(f"exceptions must contain strings, got {type(item).__name__}")
                cleaned = item.strip()
                if not cleaned:
                    continue
                if cleaned.startswith("!"):
                    raise ValueError("exception patterns cannot start with '!'")
                res.append(cleaned)
            return res
        raise ValueError(f"exceptions must be a string or list of strings, got {type(v).__name__}")

    @model_validator(mode='after')
    def expand_local_path(self):
        if self.local_path:
            self.local_path = Path(os.path.expanduser(str(self.local_path)))
        return self

class FileConfig(BaseModel):
    local_path: Path
    workspace: str = "files"
    remote_path: Optional[str] = None
    replace_symlinks: bool = False
    conflict_strategy: str = "ask"

    @model_validator(mode='after')
    def expand_local_path(self):
        if self.local_path:
            self.local_path = Path(os.path.expanduser(str(self.local_path)))
        if not self.remote_path and self.local_path:
            self.remote_path = self.local_path.name
        if self.remote_path:
            self.remote_path = unicodedata.normalize("NFC", self.remote_path.lstrip("/"))
        return self

class ClientConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    directories: List[DirectoryConfig] = Field(default_factory=lambda: [DirectoryConfig()])
    files: List[FileConfig] = Field(default_factory=list)

    @model_validator(mode='after')
    def validate_no_collisions(self):
        dir_names = set()
        for d in self.directories:
            if d.name in dir_names:
                raise ConfigError(f"Duplicate directory workspace name: {d.name}")
            dir_names.add(d.name)

        seen_files = set()
        for f in self.files:
            if f.workspace in dir_names:
                raise ConfigError(
                    f"File workspace '{f.workspace}' collides with directory workspace of the same name"
                )
            key = (f.workspace, f.remote_path)
            if key in seen_files:
                raise ConfigError(
                    f"Duplicate remote_path '{f.remote_path}' in workspace '{f.workspace}'"
                )
            seen_files.add(key)
        return self

    @property
    def SERVER_URL(self) -> str: return self.server.url
    @property
    def API_KEY(self) -> str: return self.server.api_key
    @property
    def CLIENT_ID(self) -> str: return self.server.client_id
    @property
    def DEBOUNCE_SECONDS(self) -> float: return self.server.debounce_seconds
    @property
    def POLL_INTERVAL_SECONDS(self) -> int: return self.server.poll_interval_seconds
    @property
    def REQUEST_TIMEOUT(self) -> float: return self.server.request_timeout

    def get_sync_targets(self) -> dict[str, list]:
        """Return a mapping of all configured sync targets: directories and files."""
        return {
            "directories": list(self.directories),
            "files": list(self.files),
        }

    def get_sync_targets_toml(self, portable: bool = True) -> str:
        """Return a TOML string representation of the configured sync targets."""
        return targets_to_toml(self.get_sync_targets(), portable=portable)

def get_config_path() -> Path:
    env_path = os.getenv("SYNCME_CONFIG_PATH")
    if env_path:
        return Path(env_path).expanduser()
    return Path("~/.config/SyncMe/config.toml").expanduser()

def warn_if_config_loose(path: Optional[Path] = None) -> None:
    """The config file holds the API key: tell the user when group or others can read it."""
    p = path or get_config_path()
    try:
        mode = os.stat(p).st_mode
    except OSError:
        return  # missing or unreadable: nothing to warn about
    if mode & 0o077:
        print(f"Warning: config file {p} is accessible by group or others (mode {oct(mode & 0o777)}); run: chmod 600 {p}", file=sys.stderr)

def get_central_state_dir() -> Path:
    p = Path("~/.local/share/SyncMe/state").expanduser()
    p.mkdir(parents=True, exist_ok=True)
    return p

def save_default_config(path: Optional[Path] = None):
    p = path or get_config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        return
    toml_content = """[server]
url = "http://localhost:8000"
# Create a key on the server: docker compose run --rm keys add <client-name>
api_key = ""
client_id = "default_client"
debounce_seconds = 1.0
poll_interval_seconds = 300
request_timeout = 300.0

[[directories]]
name = "default"
local_path = "~/.syncme_data"
conflict_strategy = "ask"
"""
    try:
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        # Lost a race with another process, or the path is a dangling symlink (O_EXCL never follows
        # symlinks, and exists() is False for a dangling one). Either way, leave it alone.
        return
    with os.fdopen(fd, "w") as f:
        f.write(toml_content)

def load_config(path: Optional[Path] = None) -> ClientConfig:
    p = path or get_config_path()
    if p.exists():
        with open(p, "rb") as f:
            data = tomllib.load(f)
        config = ClientConfig(**data)
    else:
        config = ClientConfig()

    if "SYNCME_SERVER_URL" in os.environ:
        config.server.url = os.environ["SYNCME_SERVER_URL"]
    if "SYNCME_API_KEY" in os.environ:
        config.server.api_key = os.environ["SYNCME_API_KEY"]
    if "SYNCME_CLIENT_ID" in os.environ:
        config.server.client_id = os.environ["SYNCME_CLIENT_ID"]
    if "SYNCME_SYNC_DIR" in os.environ:
        config.directories[0].local_path = Path(os.environ["SYNCME_SYNC_DIR"]).expanduser()
    if "SYNCME_REQUEST_TIMEOUT" in os.environ:
        config.server.request_timeout = float(os.environ["SYNCME_REQUEST_TIMEOUT"])

    return config

def get_sync_targets(config: Optional[ClientConfig] = None) -> dict[str, list]:
    """Retrieve synced directories and files for the given config or loaded config."""
    cfg = config if config is not None else load_config()
    return cfg.get_sync_targets()

def _toml_str(s: str) -> str:
    return json.dumps(s, ensure_ascii=False).replace("\x7f", "\\u007f")

def _format_portable_path(path: Path) -> str:
    try:
        rel = path.relative_to(Path.home())
        return f"~/{rel}" if str(rel) != "." else "~"
    except (ValueError, RuntimeError):
        return str(path)

def targets_to_toml(targets: dict[str, list], portable: bool = True) -> str:
    lines = []
    for d in targets.get("directories", []):
        lines.append("[[directories]]")
        lines.append(f"name = {_toml_str(d.name)}")
        p_str = _format_portable_path(d.local_path) if portable else str(d.local_path)
        lines.append(f"local_path = {_toml_str(p_str)}")
        lines.append(f"conflict_strategy = {_toml_str(d.conflict_strategy)}")
        lines.append(f"replace_symlinks = {str(d.replace_symlinks).lower()}")
        if d.exceptions:
            ex_list = ", ".join(_toml_str(e) for e in d.exceptions)
            lines.append(f"exceptions = [{ex_list}]")
        lines.append("")

    for f in targets.get("files", []):
        lines.append("[[files]]")
        lines.append(f"workspace = {_toml_str(f.workspace)}")
        p_str = _format_portable_path(f.local_path) if portable else str(f.local_path)
        lines.append(f"local_path = {_toml_str(p_str)}")
        if f.remote_path and f.remote_path != Path(f.local_path).name:
            lines.append(f"remote_path = {_toml_str(f.remote_path)}")
        lines.append(f"conflict_strategy = {_toml_str(f.conflict_strategy)}")
        lines.append(f"replace_symlinks = {str(f.replace_symlinks).lower()}")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def get_sync_targets_toml(config: Optional[ClientConfig] = None, portable: bool = True) -> str:
    """Retrieve synced targets as a TOML string suitable for pasting into config.toml."""
    cfg = config if config is not None else load_config()
    return cfg.get_sync_targets_toml(portable=portable)

settings = load_config()
