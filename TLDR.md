# SyncMe-Client TL;DR

Quickstart cheat sheet for installing, configuring, and operating SyncMe-Client.

---

## 1. Installation

### Option A: Virtual Environment with `uv` (Recommended)

```bash
cd SyncMe-Client

# Create virtual environment
uv venv

# Activate virtual environment:
# Bash / Zsh:
source .venv/bin/activate
# Fish shell:
source .venv/bin/activate.fish

# Install in editable mode
uv pip install -e .
```

*(Standard Python alternative: `python3 -m venv .venv`, activate as above, then `pip install -e .`)*

### Option B: Standalone Global Tool with `uv tool`

```bash
cd SyncMe-Client
uv tool install --editable .
```

---

## 2. Configuration (`~/.config/SyncMe/config.toml`)

Create or edit `~/.config/SyncMe/config.toml` to configure server connection details and multiple local synchronization workspaces:

```toml
[server]
url = "http://localhost:8000"
api_key = ""  # paste the key from the server here
client_id = "laptop-client"
debounce_seconds = 1.0
poll_interval_seconds = 300
request_timeout = 300.0

[[directories]]
name = "notes"
local_path = "~/Notes"
conflict_strategy = "ask"
replace_symlinks = false

[[directories]]
name = "docs"
local_path = "~/Documents"
conflict_strategy = "server"
replace_symlinks = true

[[directories]]
name = "code"
local_path = "~/Projects"
conflict_strategy = "ask"

[[files]]
local_path = "~/.bashrc"
workspace = "dotfiles"
remote_path = ".bashrc"
```

> **Keys and URLs:** Each client needs its own key, created on the server with `docker compose run --rm keys add <name>`. There is no default key, `default_token` is rejected, and a key with spaces, line breaks or other non-printable characters is rejected too (it is never trimmed). The example above ships with an empty `api_key` on purpose: copied verbatim, the client refuses to start until you paste your key. The server URL must be `https://`, except for `localhost`, `127.0.0.1` and `::1`, and must not contain a user name or password. `init`, `run` and `sync` refuse to start otherwise. The `SYNCME_API_KEY` environment variable still overrides `api_key` on the client.
>
> **Upgrading:** after the server is updated, every client needs its own key before it can sync: create it on the server, set `api_key` (and an `https://` `url`), run `syncme-client sync` once, then restart the systemd user service.

> **Note:** The configuration file is automatically generated with defaults (mode `0600`, empty `api_key`) on first run if it does not exist. You can also override the configuration path using the `SYNCME_CONFIG_PATH` environment variable.

> **File Groups (`[[files]]`):** Managed by `FileGroupEngine`. State databases are stored centrally at `~/.local/share/SyncMe/state/<workspace>.db` rather than creating `.syncme/` folders in individual parent directories.

---

## 3. Initialization

Before syncing, initialize the `.syncme/` state database and perform the initial two-way reconciliation:

```bash
# Initialize all configured directories
syncme-client init

# Initialize a specific directory only
syncme-client init -d notes
# or: syncme-client init --directory notes --conflict-strategy server
```

---

## 4. Execution & Operations

Commands run globally across all configured directories by default, or can target a single directory using `-d` / `--directory <name>`.

### Run as Live Daemon (Watcher + SSE Stream)
Runs background filesystem watching and real-time SSE synchronization.

```bash
# Run for all directories concurrently
syncme-client run

# Run for a single directory
syncme-client run -d notes
# or: syncme-client run --directory notes
```

### Run One-Time Sync
Performs a single reconciliation pass and exits immediately.

```bash
# Sync all directories
syncme-client sync

# Sync a single directory
syncme-client sync -d docs
# or: syncme-client sync --directory docs
```

### Check Status
Displays tracked file counts and status.

```bash
# Status for all directories
syncme-client status

# Status for a single directory
syncme-client status -d code
# or: syncme-client status --directory code
```

### Run as a Systemd User Service

Run the client daemon persistently in the background using systemd:

```bash
# 1. Link or copy service unit
mkdir -p ~/.config/systemd/user
ln -sf "$(pwd)/systemd/syncme-client.service" ~/.config/systemd/user/
# or: cp systemd/syncme-client.service ~/.config/systemd/user/

# 2. Reload daemon and enable service
systemctl --user daemon-reload
systemctl --user enable --now syncme-client.service

# 3. View live logs
journalctl --user -u syncme-client.service -f

# 4. Keep running after logout / start on boot
loginctl enable-linger $USER
```

---

## 5. Local Archive Commands

Files overwritten or deleted during remote synchronization are safely preserved in `.syncme/archive/`.

```bash
# List archives across all directories
syncme-client archive list

# List archives for a specific directory
syncme-client archive list -d notes

# Restore an archived file (specify -d / --directory if multiple directories are configured)
syncme-client archive restore notes__todo.txt_1727860000 notes/todo.txt -d notes
```

---

## 6. Ignore Rules (`.smignore`)

Create a `.smignore` file in the root of any sync directory (e.g. `~/Notes/.smignore`):

```gitignore
# Ignore python virtual environments and caches
.venv/
__pycache__/
*.pyc

# Ignore build output and logs
dist/
build/
*.log
```

Default rules automatically ignore `.syncme/`, `.git/`, `.smignore`, and `*.conflict.*`.

---

## 7. Conflict Management & Resolution (`conflicts`)

When concurrent modifications are detected during synchronization, the local version is safely preserved alongside the original as `<stem>.conflict.<timestamp><suffix>` (e.g. `notes.conflict.1727860000.txt`). The conflict is persistently queued with `pending` status in the workspace's local SQLite state database (`.syncme/local_state.db` for directory workspaces, or `~/.local/share/SyncMe/state/<workspace>.db` for file groups).

> **Note:** Conflict files matching `*.conflict.*` are ignored by default and are never synced to the server.

### List Pending Conflicts
List all unresolved conflicts across all configured workspaces:

```bash
syncme-client conflicts list
```

*Example output:*
```text
Workspace: notes
  [1] meeting-notes.md
```

### Count Pending Conflicts
Check pending conflict counts for terminal scripts and desktop status bars:

```bash
# Plain numeric count (e.g. "0" or "2")
syncme-client conflicts count

# Waybar / status bar JSON output
syncme-client conflicts count --format waybar
# Outputs: {"text": "⚠️ 2", "tooltip": "2 conflicts pending", "class": "warning"}
# When clear: {"text": "✔", "tooltip": "All synced", "class": "synced"}

# Full JSON output
syncme-client conflicts count --format json
```

### Interactive Resolution Wizard
Step through pending conflicts one-by-one to inspect and resolve them:

```bash
syncme-client conflicts resolve
```

Displays a colorized unified diff directly in the terminal window (with a 1MB file limit, binary detection, ANSI escape sanitization, and capped at 60 lines), followed by action options:
- `[s]erver`: Server version wins. Discards the local `.conflict` copy and marks the record resolved.
- `[l]ocal`: Local conflict version wins. Overwrites the file with the conflict copy, uploads it to the server, removes the `.conflict` file, and marks the conflict resolved.
- `[n]ext`: Skip to the next conflict without resolving.
- `[q]uit`: Exit the resolution wizard immediately.

### Waybar Integration (Hyprland / Wayland)

Add a custom widget to your Waybar configuration:

**`~/.config/waybar/modules.json`**:
```jsonc
"custom/syncme": {
    "exec": "syncme-client conflicts count --format waybar",
    "return-type": "json",
    "interval": 15,
    "format": "{}",
    "on-click": "kitty --class dotfiles-floating -e syncme-client conflicts resolve",
    "tooltip": true
}
```

**`~/.config/waybar/themes/ml4w-modern/style.css`**:
```css
#custom-syncme.warning {
    color: #f38ba8;
    font-weight: bold;
}

#custom-syncme.synced {
    color: #a6e3a1;
}
```

**`~/.config/waybar/themes/ml4w-modern/config`**:
```
"custom/syncme",
```

#### Floating Window in Hyprland

Setting `"on-click": "kitty --class dotfiles-floating -e syncme-client conflicts resolve"` opens the wizard in a floating terminal if your Hyprland configuration contains a rule for `dotfiles-floating` (e.g. `windowrule = float, class:^(dotfiles-floating)$` and `windowrule = center, class:^(dotfiles-floating)$`).

---

## 8. File History & Revision Rollback

Track changes and revert files to previous server revisions on demand.

### View Revision History
Displays a chronological revision table (revision, timestamp, size, client name, deletion status):

```bash
# View last 50 revisions (default)
syncme-client history notes/todo.txt

# Specify revision limit and workspace directory
syncme-client history notes/todo.txt -l 10 -d notes
# or: syncme-client history ~/.bashrc --limit 20
```

- Arguments:
  - `FILE_PATH`: Local or relative path to the file.
- Options:
  - `-l, --limit <INTEGER>`: Maximum number of revisions to display (default: 50).
  - `-d, --directory <NAME>`: Specific directory workspace if path is ambiguous.

### Roll Back to Historical Revision
Rolls back a file to an exact historical revision on the server, creates a new monotonic revision, and broadcasts the restored file to all connected clients in real time:

```bash
# Roll back file to revision 4
syncme-client restore notes/todo.txt -r 4

# Specify workspace directory if path is ambiguous
syncme-client restore notes/todo.txt --revision 4 -d notes
syncme-client restore ~/.bashrc -r 2
```

- Arguments:
  - `FILE_PATH`: Local or relative path to the file.
- Options:
  - `-r, --revision <INTEGER>`: Revision number to restore (required).
  - `-d, --directory <NAME>`: Specific directory workspace if path is ambiguous.
