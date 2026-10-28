# SyncMe-Client

SyncMe-Client is the client daemon and command-line interface for the SyncMe synchronization system. Written in Python 3.12, it provides recursive filesystem monitoring, multi-directory workspace synchronization, event debouncing, continuous Server-Sent Events (SSE) streaming, optimistic concurrency reconciliation, deterministic conflict resolution, and local file archival.

---

## Table of Contents
- [Architecture & Core Mechanics](#architecture--core-mechanics)
  - [Multi-Directory Workspaces](#multi-directory-workspaces)
  - [Filesystem Monitoring & Debouncing](#filesystem-monitoring--debouncing)
  - [SSE Event Streaming](#sse-event-streaming)
  - [Reconciliation Engine](#reconciliation-engine)
  - [Initial Synchronization](#initial-synchronization)
  - [Conflict Detection & Strategies](#conflict-detection--strategies)
  - [Ignore Pattern Matching (.smignore)](#ignore-pattern-matching-smignore)
  - [Local Archive Management](#local-archive-management)
- [Configuration](#configuration)
  - [Configuration File (`config.toml`)](#configuration-file-configtoml)
  - [Environment Variables](#environment-variables)
  - [Upgrading to Per-Client Keys](#upgrading-to-per-client-keys)
  - [Local State Directory Layout](#local-state-directory-layout)
- [CLI Command Guide](#cli-command-guide)
  - [`init`](#syncme-client-init)
  - [`run`](#syncme-client-run)
  - [`sync`](#syncme-client-sync)
  - [`status`](#syncme-client-status)
  - [`archive list`](#syncme-client-archive-list)
  - [`archive restore`](#syncme-client-archive-restore)
  - [`history`](#syncme-client-history)
  - [`restore`](#syncme-client-restore)
  - [`conflicts list`](#syncme-client-conflicts-list)
  - [`conflicts count`](#syncme-client-conflicts-count)
  - [`conflicts resolve`](#syncme-client-conflicts-resolve)
  - [Waybar / Status Bar Integration](#waybar--status-bar-integration)
  - [Exit codes](#exit-codes)
- [Running as a Systemd User Service](#running-as-a-systemd-user-service)
  - [Upgrading an existing install](#upgrading-an-existing-install)
- [Development & Testing](#development--testing)

---

## Architecture & Core Mechanics

### Multi-Directory Workspaces
SyncMe-Client supports synchronizing multiple independent directories simultaneously. Each directory defined in the client configuration corresponds to a distinct `workspace_id` on the server:
- **Isolation:** Each synchronized directory maintains its own isolated `.syncme/` state database and local archive directory.
- **Concurrent Daemon Execution:** When running in daemon mode (`syncme-client run`), all configured directories (or a specifically selected directory) run concurrently via asynchronous task groups (`asyncio.gather`), each handling its own filesystem watcher and SSE event loop.
- **File Groups (`FileGroupEngine`):** In addition to full directory trees, arbitrary individual files can be synchronized under a logical workspace via `[[files]]` blocks. State for these files is managed centrally in `~/.local/share/SyncMe/state/<workspace>.db` so user folders (such as `$HOME`) remain clutter-free.

### Filesystem Monitoring & Debouncing
The client employs [Watchdog](https://github.com/gorakhargosh/watchdog) to recursively observe each local sync directory (`local_path`).
- Event types handled: `created`, `modified`, `deleted`, `moved`.
- **Debouncing:** Rapid filesystem modifications (e.g. editor temporary files or multi-write flushes) are queued and debounced over a configurable window (`debounce_seconds`, default: `1.0s`). Once settled, unique pending changes trigger synchronization.

### SSE Event Streaming
When running in daemon mode (`syncme-client run`), the client opens an asynchronous HTTP streaming connection to `GET /api/v1/workspaces/{workspace_id}/events` using `httpx` for each workspace.
- As soon as the server broadcasts a `file_updated` or `file_deleted` event for that workspace from any remote client, the client immediately wakes up and executes `engine.reconcile()`.
- An internal `asyncio.Lock` per workspace ensures that watcher events, SSE notifications, and manual sync commands run serially without interleaving.
- **Reconnect with backoff:** when the stream ends or breaks (server restart, network drop, proxy timeout, any HTTP error status other than 401, including a proxy's 403), the client reconnects after 1 s, doubling the wait on every attempt up to 60 s. The wait starts over at 1 s only after a stream that stayed open for at least 30 s (`sse_healthy_seconds`); receiving an event does not reset it, so a server that closes the stream after every event is not hammered once a second. The stream has a 15 s connect timeout and a 60 s read timeout: the server sends a keep-alive ping every 15 s, so 60 s of silence means a dead connection and triggers a reconnect. The log line of an interrupted stream names the HTTP status and path when there is one.
- **Catch-up after a reconnect:** once the stream is open again (the client treats the accepted HTTP response as a first, synthetic `connected` event) the client runs `reconcile()`, like for every real event. The catch-up therefore runs after the stream is open, so a change made while the stream was down or being re-opened is not lost.
- A reconcile that fails for a reason other than a rejected key is logged (`Reconcile failed: ...`) and does not stop the event stream, watcher or periodic poll loop; the next event, batch or poll tries again. Only HTTP 401 ends the daemon, see [Exit codes](#exit-codes). The one reconcile at the start of `run` is different: if that pass itself raises (for example the state request fails with a 503 or 403, or the server is unreachable), the process exits non-zero and systemd restarts it. Errors on individual files are only warnings and do not end the pass.

### Reconciliation Engine
The `SyncEngine.reconcile()` procedure executes in structured passes for each workspace:
1. **Fetch Remote Changes:** Requests `GET /api/v1/workspaces/{workspace_id}/sync/state?since_revision=<last_revision>`.
2. **Process Remote Modifications & Deletions:**
   - Deleted remotely: the local file is copied to `.syncme/archive/` and removed from disk and database. A file the client has no record of, or whose local revision is at least as new as the deletion, is kept (an untracked file with a warning), because a deletion can be replayed while a failed download holds the stored revision back; directory workspaces also keep a file that changed since the last sync.
   - Updated remotely:
     - If local file exists, was modified since last sync, and server revision is newer, a **conflict** is declared: the remote version is downloaded and verified first, then the local file is renamed to `<stem>.conflict.<timestamp><suffix>` and the remote version takes its place.
     - A regular local file the client has no record of (for example after an `init` in which a file failed) is never overwritten silently: it is adopted without a download if it is identical to the server version, otherwise it is a conflict.
     - Otherwise, the remote version is downloaded and local metadata is updated.
     - A download is checked against the SHA-256 from the server state before anything local is renamed, archived, replaced or written. If the download fails or does not match, the local file stays exactly as it was and the entry is retried on the next reconcile (the stored server revision is not advanced). This holds for directory workspaces and for single-file `[[files]]` workspaces alike.
3. **Propagate Local Deletions:** Compares tracked files in SQLite against disk. Any missing local files trigger `DELETE /api/v1/workspaces/{workspace_id}/files/{path}` on the server using `If-Match: <server_revision>`.
4. **Propagate Local Additions & Edits:** Scans local disk. New or modified files (mtime > recorded mtime) are hashed (SHA-256) and uploaded via `PUT /api/v1/workspaces/{workspace_id}/files/{path}` with `If-Match: <server_revision>`.

### Initial Synchronization
When setting up a new sync directory or reconnecting an existing folder, `syncme-client init` runs `SyncEngine.initial_sync()`:
- Inspects all local files and queries complete server state (`since_revision=0`) for the workspace.
- **Local only:** Uploaded to server with `If-Match: 0`.
- **Server only:** Downloaded to local folder.
- **Both exist:** Compares SHA-256 hashes. If identical, metadata is synced without file transfer. If hashes differ, a conflict is raised.

### Conflict Detection & Strategies

#### Initial Synchronization Discrepancies
During `syncme-client init`, file discrepancies between local files and the server are resolved according to `--conflict-strategy` (or per-workspace configuration in `config.toml`):
- `ask` (default): Displays local and remote file size/mtime and prompts for choice interactively. Answering `S` or `L` (uppercase) applies server-wins or local-wins to this and every remaining conflict in the same `init` run, across all directories, without asking again.
- `server` (`s`): Server version wins. Local file is moved to `.syncme/archive/` and remote file is downloaded.
- `local` (`l`): Local version wins. Local file is uploaded to the server, overwriting server state with `If-Match: <server_revision>`.
- `both` (`b`): Keeps both. The remote file is downloaded and verified first; then the local file is renamed to `<stem>.conflict.<timestamp><suffix>` and the remote file takes its place. If the download fails, nothing local changes and the file is counted as failed.

#### Real-Time Conflict Lifecycle
During live synchronization (`syncme-client run` and `syncme-client sync`), conflicts are managed autonomously with strict safety guarantees:
1. **Verification to Temporary File:** When remote updates arrive for a file that has been modified locally since the last sync (or is an untracked file with differing content), the remote version is first downloaded and verified against the expected SHA-256 hash into a temporary file (`.syncme-tmp-*` in the file's parent directory). If the download or checksum fails, disk contents are untouched, the stored server revision is not advanced, and the file is retried on the next reconciliation pass.
2. **Preserving Local Divergence:** Once verification succeeds, the local file is renamed to `<stem>.conflict.<timestamp><suffix>` (with collision counter `<stem>.conflict.<timestamp>_<cnt><suffix>` if needed). The verified remote file is then atomically swapped into the destination path (`os.replace`), preserving file permission modes.
3. **Persistent SQLite Tracking:** The conflict is recorded in the workspace's SQLite state database in the `conflicts` table (`status='pending'`, storing `workspace`, `rel_path`, `server_file`, `conflict_file`, and `detected_at`). A unique partial index ensures duplicate pending records are not created.
4. **Desktop Alert (`notify-send`):** An immediate desktop notification is dispatched using `notify-send -u critical -a SyncMe -i dialog-warning` with interactive action buttons:
   - `🔍 Resolve with Meld`: Launches the external visual merge tool `meld <server_file> <conflict_file>`. When Meld exits successfully (exit code 0), the resolved file is uploaded to the server, the local `.conflict` copy is unlinked, and the database record is updated to `status='resolved'` (`resolution='meld'`).
   - `💾 Keep my local version`: Copies the local `.conflict` file over the active file, uploads it to the server with a new revision, deletes the `.conflict` file, and marks the conflict resolved (`resolution='local'`).
   - `☁️ Keep server version`: Keeps the server version on disk, deletes the local `.conflict` file, and marks the conflict resolved (`resolution='server'`).

   > [!WARNING]
   > **Meld Save Requirement & Data Loss Warning:** Meld exits with return code `0` even when closed without saving. If closed without saving, the server version is re-uploaded and the local `.conflict` copy is permanently deleted, causing permanent loss of your local edits. Always save your merged file in Meld before closing the window.
5. **Interactive Terminal Wizard (`conflicts resolve`):** Conflicts can be inspected and resolved directly from the terminal. The wizard renders unified diffs using `format_inline_diff`, which enforces:
   - **1 MB File Safety Limit:** Files larger than 1 MB (`1_048_576` bytes) skip inline diff generation to prevent terminal hangs.
   - **Binary File Detection:** Inspects initial bytes for null bytes (`\0`) and flags binary file differences cleanly.
   - **ANSI Sanitization:** Strips/sanitizes terminal escape sequences (`\x1b`) and control characters from file content to avoid terminal injection.
   - **Bounded Output:** Colorized unified diff output is capped at 60 lines with a truncation indicator for large diffs.

### Ignore Pattern Matching (.smignore)
SyncMe-Client prevents syncing unwanted files using gitignore-style matching powered by `pathspec`:
- **Default Ignores:** Always ignores `.smignore`, `.syncme/`, `.git/`, and `*.conflict.*`.
- **Custom Patterns:** Rules placed in `.smignore` in the root of each sync directory are evaluated against all relative file paths.
- Lines starting with `#` or `//` are treated as comments. Blank lines are ignored.
- Inline prefixes like `(?i)` and `(?d)` are automatically stripped for regex compatibility.

### Local Archive Management
Before overwriting or deleting any file as a result of remote synchronization, SyncMe copies the active file into `.syncme/archive/`:
- Archive naming convention: `<sanitized_path>_<timestamp>` (forward and back slashes replaced by `__`).
- Files in the archive can be listed with `syncme-client archive list` and restored using `syncme-client archive restore`.

---

## Configuration

SyncMe-Client configuration is stored in a TOML file located at `~/.config/SyncMe/config.toml` by default, or loaded from the path specified by the `SYNCME_CONFIG_PATH` environment variable.

If the configuration file does not exist when executing `syncme-client init`, a default configuration file is automatically created (mode `0600`, with an empty `api_key`). `init`, `run` and `sync` refuse to start until a real per-client key is configured and the server URL uses `https://` (plain `http://` is only accepted for `localhost`, `127.0.0.1` and `::1`). The server URL must not contain a user name or password in front of the host name. `init`, `run` and `sync` also print a warning to stderr when the config file is accessible by group or others (any of the mode bits `077` set), because it holds the API key: fix it with `chmod 600 ~/.config/SyncMe/config.toml`. The warning names the path and the mode but never prints the file's content; `status` and the `archive` commands do not warn. The example below ships with an empty `api_key` on purpose: copied verbatim, the client refuses to start until you paste the key from the server.

### Configuration File (`config.toml`)

Here is an example `~/.config/SyncMe/config.toml` illustrating multi-directory configuration:

```toml
[server]
url = "http://localhost:8000"
api_key = ""  # paste the key from the server here
client_id = "default_client"
debounce_seconds = 1.0
poll_interval_seconds = 300
request_timeout = 300.0

[[directories]]
name = "default"
local_path = "~/.syncme_data"
conflict_strategy = "ask"

[[directories]]
name = "work"
local_path = "~/Sync/work"
conflict_strategy = "server"
replace_symlinks = true

[[directories]]
name = "personal"
local_path = "~/Sync/personal"
conflict_strategy = "ask"

[[files]]
local_path = "~/.bashrc"
workspace = "dotfiles"
remote_path = ".bashrc"

[[files]]
local_path = "~/.vimrc"
workspace = "dotfiles"
remote_path = ".vimrc"
```

#### `[server]` Section

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `url` | `string` | `"http://localhost:8000"` | Base URL of the SyncMe-Server instance. `https://` is required, except for `localhost`, `127.0.0.1` and `::1`. |
| `api_key` | `string` | `""` | Per-client key created on the server (`docker compose run --rm keys add <name>`). No default; `""` and `default_token` are rejected, and so is any key containing spaces, tabs, line breaks, other control characters or non-ASCII characters (the key is used exactly as written, never trimmed). |
| `client_id` | `string` | `"default_client"` | Label for this client: 1-64 characters from `A-Z a-z 0-9 . _ -` (anything else, including spaces, line breaks and non-ASCII characters, is rejected by `init`, `run` and `sync`). Still sent in the `X-Client-ID` header but ignored by the server: the server identifies this client by the name its key was created with. |
| `debounce_seconds` | `float` | `1.0` | Debounce window (seconds) for batching local filesystem events. |
| `poll_interval_seconds` | `integer` | `300` | Periodic fallback polling interval (seconds). |
| `request_timeout` | `float` | `300.0` | Request timeout (seconds) for API operations (connect, read, write). |

#### `[[directories]]` Section

You can specify multiple `[[directories]]` blocks to configure multiple synchronized workspaces:

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `name` | `string` | `"default"` | Name of the directory, corresponding to the server's `workspace_id`. |
| `local_path` | `path` | `"~/.syncme_data"` | Local filesystem directory path (supports `~` expansion). |
| `conflict_strategy` | `string` | `"ask"` | Default conflict strategy (`ask`, `server`, `local`, `both`) during `init`. |
| `replace_symlinks` | `boolean` | `false` | Whether to upload symlink targets and replace local symlinks with remote files. |
| `exceptions` | `string` or `list` | `[]` | Local files or patterns excluded from synchronization for this directory (e.g. `"fil1,fil2"` or `["fil1", "fil2"]`). |

#### `[[files]]` Section

You can also specify `[[files]]` blocks to synchronize individual files across different arbitrary locations under a shared logical workspace ID. Unlike directory workspaces, state for file groups (`FileGroupEngine`) is kept centrally in `~/.local/share/SyncMe/state/<workspace>.db`, ensuring no `.syncme/` folders are injected into arbitrary parent paths (like your home folder).

| Field | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `local_path` | `path` | (Required) | Local filesystem path to the file (supports `~` expansion). |
| `workspace` | `string` | `"files"` | The logical workspace ID these files share on the server. |
| `remote_path` | `string` | `local_path.name` | The path/name of this file on the server. |
| `conflict_strategy` | `string` | `"ask"` | Default conflict strategy (`ask`, `server`, `local`, `both`) during `init`. |
| `replace_symlinks` | `boolean` | `false` | Whether to replace local symlinks with remote files. |

### Environment Variables

Environment variables can override values specified in `config.toml`:

| Environment Variable | Description |
| :--- | :--- |
| `SYNCME_CONFIG_PATH` | Path to custom TOML configuration file (default: `~/.config/SyncMe/config.toml`). |
| `SYNCME_SERVER_URL` | Overrides `server.url`. |
| `SYNCME_API_KEY` | Overrides `server.api_key` (client-side override; still supported, and subject to the same checks). |
| `SYNCME_CLIENT_ID` | Overrides `server.client_id` (same character rules). |
| `SYNCME_SYNC_DIR` | Overrides `local_path` for the first configured directory. |
| `SYNCME_REQUEST_TIMEOUT` | Overrides `server.request_timeout`. |

### Upgrading to Per-Client Keys

The server no longer has a single shared API key. After the server is updated, every client needs its own key before it can sync:

1. On the server, create a key for each machine: `docker compose run --rm keys add <name>` (the key is printed once).
2. On the client, set `api_key` in `~/.config/SyncMe/config.toml` (or export `SYNCME_API_KEY`) and make sure `url` starts with `https://`.
3. Run `syncme-client sync` once, then restart the systemd user service if you use one.

### Local State Directory Layout

#### Directory Workspaces (`[[directories]]`)
Within each synchronized local directory (`local_path`), SyncMe maintains its internal state:

```
<local_path>/
├── .syncme/
│   ├── local_state.db       # SQLite DB tracking files, hashes, mtimes, revisions, conflicts
│   └── archive/             # Archived local files before overwrite or remote deletion
├── .smignore                # User-defined ignore patterns (optional)
└── <your-files-and-folders>
```

#### File Group Workspaces (`[[files]]`)
For individual files synchronized under a shared logical workspace via `FileGroupEngine`, state is kept in the central user state directory:

```
~/.local/share/SyncMe/state/
├── <workspace>.db           # SQLite DB per file workspace (e.g. dotfiles.db)
└── ...
```

---

## CLI Command Guide

Most workspace-targeted commands (`init`, `run`, `sync`, `status`, `archive`, `history`, `restore`) accept `--directory` / `-d <NAME>` to operate on a single configured directory workspace, running across all configured directories when omitted. In contrast, `conflicts` commands (`conflicts list`, `conflicts count`, `conflicts resolve`) operate globally across all local workspace databases.

Network and authentication requirements:
- **Network operations (`api_key` & HTTPS required):** `init`, `run`, `sync`, `history`, `restore`, and `conflicts resolve` (when pushing a local resolution) connect to the server.
- **Local operations (offline, no key needed):** `status`, `archive list`, `archive restore`, `conflicts list`, and `conflicts count` operate purely on local disk and SQLite state.

### `syncme-client init`
Initializes `.syncme/` state database and performs initial two-way reconciliation.

```bash
# Initialize all configured directories
syncme-client init

# Initialize a specific directory workspace
syncme-client init --directory work --conflict-strategy server
syncme-client init -d default
```

- Options:
  - `-d, --directory <NAME>`: Specific directory workspace to initialize.
  - `--conflict-strategy [ask|server|local|both]`: Strategy for handling file discrepancies during initial sync.
- Prompts for confirmation if the directory is already initialized.
- Displays summary of uploaded, downloaded, conflicted, and unchanged files.

### `syncme-client run`
Runs the long-lived synchronization daemon.

```bash
# Run daemon for all configured directories concurrently
syncme-client run

# Run daemon for a specific directory workspace only
syncme-client run --directory work
syncme-client run -d personal
```

- Options:
  - `-d, --directory <NAME>`: Specific directory workspace to run.
- Performs an immediate reconciliation pass upon startup.
- Concurrently launches local Watchdog event loops and remote SSE listeners for all targeted directories.
- Pushes local updates and pulls remote updates continuously in real-time.
- For running continuously in the background across restarts and logins, see [Running as a Systemd User Service](#running-as-a-systemd-user-service).

### `syncme-client sync`
Executes a single, one-off reconciliation pass and exits.

```bash
# Perform one-off sync for all configured directories
syncme-client sync

# Perform one-off sync for a specific directory
syncme-client sync --directory work
syncme-client sync -d default
```

- Options:
  - `-d, --directory <NAME>`: Specific directory workspace to synchronize.

### `syncme-client status`
Displays current synchronization status and tracked file counts.

```bash
# Display status for all configured directories
syncme-client status

# Display status for a specific directory
syncme-client status --directory work
syncme-client status -d default
```

- Options:
  - `-d, --directory <NAME>`: Specific directory workspace to inspect.

### `syncme-client archive list`
Lists all files currently preserved in the local `.syncme/archive/`.

```bash
# List archives across all configured directories
syncme-client archive list

# List archives for a specific directory
syncme-client archive list --directory work
syncme-client archive list -d default
```

- Options:
  - `-d, --directory <NAME>`: Specific directory workspace to list archives from.

### `syncme-client archive restore`
Restores a file from the local archive back into the active synchronized tree.

```bash
# Restore file in a single-directory setup
syncme-client archive restore notes__todo.txt_1727860000

# Restore file in a specific directory workspace
syncme-client archive restore notes__todo.txt_1727860000 notes/todo.txt --directory work
syncme-client archive restore notes__todo.txt_1727860000 -d default
```

- Arguments:
  - `ARCHIVE_NAME`: Name of the archive file inside `.syncme/archive/`.
  - `TARGET_PATH` (optional): Relative path where file should be restored. If omitted, the path is deduced from the sanitized archive filename.
- Options:
  - `-d, --directory <NAME>`: Specific directory workspace to restore into. Required if multiple directories are configured in `config.toml`.

### `syncme-client history`
Displays a tabular change log of all revisions for a tracked file on the server (like `git log` for a file):

```bash
syncme-client history notes/todo.txt
syncme-client history ~/.bashrc -l 10
syncme-client history notes/todo.txt --directory work
```

- Arguments:
  - `FILE_PATH`: Local or relative path to the file.
- Options:
  - `-l, --limit <INTEGER>`: Maximum number of revisions to display (default: 50).
  - `-d, --directory <NAME>`: Specific directory workspace if path is ambiguous.

### `syncme-client restore`
Restores a file to an exact historical revision on the server (like `git checkout <rev> -- <file>`). The server rolls back the file, assigns a new monotonic revision, and broadcasts the restored file to all connected clients in real time:

```bash
syncme-client restore notes/todo.txt --revision 4
syncme-client restore ~/.bashrc -r 2
```

- Arguments:
  - `FILE_PATH`: Local or relative path to the file.
- Options:
  - `-r, --revision <INTEGER>`: Revision number to restore (required).
  - `-d, --directory <NAME>`: Specific directory workspace if path is ambiguous.

### `syncme-client conflicts list`
Lists all pending, unresolved file conflicts across all configured workspaces (including both directory and file-group workspaces):

```bash
syncme-client conflicts list
```

*Example output:*
```text
Workspace: default
  [1] notes/todo.txt
Workspace: dotfiles
  [2] .bashrc
```

### `syncme-client conflicts count`
Outputs the count of pending conflicts across all workspaces. Useful for status bars, terminal prompts, and scripts:

```bash
# Plain integer count (e.g. "0" or "2")
syncme-client conflicts count

# Waybar / status bar JSON output
syncme-client conflicts count --format waybar

# Full JSON output including conflict record details
syncme-client conflicts count --format json
```

- Options:
  - `--format [plain|waybar|json]`: Output format (default: `plain`).
    - `plain`: Prints just the integer count.
    - `waybar`: Prints JSON containing `text`, `tooltip`, and `class` (`warning` if conflicts exist, `synced` if clear).
    - `json`: Prints JSON containing total `count` and an array of all pending conflict objects.

### `syncme-client conflicts resolve`
Launches the interactive terminal wizard to inspect and resolve pending conflicts step-by-step:

```bash
syncme-client conflicts resolve
```

The wizard displays an inline colorized unified diff using `format_inline_diff` (1 MB safety limit, binary detection, ANSI escape sanitization, capped at 60 lines), followed by action prompts:
- `[s]erver`: Server version wins. Discards the local `.conflict` copy and marks the record resolved in the database.
- `[l]ocal`: Local conflict copy wins. Overwrites the active file with the conflict copy, uploads it to the server, removes the `.conflict` file, updates local state, and marks the conflict resolved.
- `[n]ext`: Skips to the next pending conflict without modifying anything.
- `[q]uit`: Exits the resolution wizard immediately.

### Waybar / Status Bar Integration

SyncMe-Client integrates directly into desktop status bars such as Waybar (on Hyprland / Sway / Wayland) to provide real-time conflict indicators and one-click interactive resolution.

#### 1. Waybar Module Configuration (`~/.config/waybar/modules.json`)
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

#### 2. Styling (`~/.config/waybar/style.css`)
```css
#custom-syncme.warning {
    color: #f38ba8;
    font-weight: bold;
}

#custom-syncme.synced {
    color: #a6e3a1;
}
```

#### 3. Bar Inclusion (`~/.config/waybar/config`)
Add `"custom/syncme"` to your `modules-right` or `modules-left` list.

> [!TIP]
> **Hyprland Floating Window Tip:** If you configure a floating window rule for `dotfiles-floating` in Hyprland (e.g. `windowrule = float, class:^(dotfiles-floating)$` and `windowrule = center, class:^(dotfiles-floating)$`), setting `"on-click": "kitty --class dotfiles-floating -e syncme-client conflicts resolve"` opens the resolution wizard in a centered floating terminal window.

### Exit codes

| Code | Meaning |
| :--- | :--- |
| `0` | Success. |
| `1` | Configuration or usage error (missing or malformed key, URL or `client_id`, directory not initialized, ...). |
| `78` | The server rejected the API key (HTTP 401), for instance because the key was revoked. One clear message is printed and the process stops. |

`78` is the sysexits code `EX_CONFIG`. The systemd unit sets `RestartPreventExitStatus=78`, so systemd does not restart the daemon every five seconds with a key that will never work again. Any other failure is still restarted (`Restart=on-failure`).

Only HTTP 401 means "revoked key": the server answers 401 (never 403) for a bad key. A 403 comes from something in front of the server, such as a proxy, WAF or HAProxy `http-request deny` rule, and is treated like any other HTTP error, so the daemon is not stopped for good by it.

What happens with other errors:

- Inside a reconcile, a failed upload (anything but 409) prints `Warning: upload of <path> failed: HTTP <status>`, a failed server-side delete prints a warning, and a failed download skips that entry and retries it on the next reconcile. A 409 (someone else changed the file) stays silent and is resolved by the next reconcile.
- In the daemon (`syncme-client run`), a reconcile that fails in the event stream, watcher or periodic poll loop is logged and the loop carries on. A broken event stream reconnects with backoff.
- The first reconcile when `run` starts is fail-fast when the pass itself raises (for example the state request fails with a 503 or 403, or the server is unreachable): the process ends with a non-zero exit code other than 78, and systemd restarts it (`Restart=on-failure`). A per-file error in that pass is only a warning, as above, and the daemon carries on.
- `syncme-client sync` and `init` exit non-zero when their single pass itself raises a non-auth error (same examples). Per-file errors are warnings: `sync` still prints `Sync complete`, `init` counts them under `Failed`, and the exit code stays 0.
- HTTP 401 anywhere (including the event stream, watcher and poll loops) ends the process with exit code 78.

---

## Running as a Systemd User Service

SyncMe-Client can be configured as a `systemd` user service to run the synchronization daemon continuously in the background on Linux systems without requiring root privileges.

The service definition file is provided at `systemd/syncme-client.service`:
```ini
[Unit]
Description=SyncMe Client Daemon
After=network.target

[Service]
Type=simple
Environment=PYTHONUNBUFFERED=1
ExecStart=%h/.local/bin/syncme-client run
Restart=on-failure
RestartSec=5
RestartPreventExitStatus=78

[Install]
WantedBy=default.target
```

### 1. Install the Service File

Ensure the systemd user service directory exists:
```bash
mkdir -p ~/.config/systemd/user
```

You can either link or copy `systemd/syncme-client.service` to `~/.config/systemd/user/syncme-client.service`:

- **Option A: Symbolic Link (recommended for development)**
  ```bash
  ln -sf "$(pwd)/systemd/syncme-client.service" ~/.config/systemd/user/syncme-client.service
  ```
- **Option B: Copy File**
  ```bash
  cp systemd/syncme-client.service ~/.config/systemd/user/syncme-client.service
  ```

> **Note:** The unit assumes `syncme-client` is installed in `%h/.local/bin/syncme-client` (e.g. via `uv tool install --editable .` or `pip install --user`). If installed in a virtual environment or another path, update `ExecStart` accordingly.

### 2. Reload Systemd and Enable the Service

Reload the systemd user daemon to recognize the unit file:
```bash
systemctl --user daemon-reload
```

Enable and start the service immediately:
```bash
systemctl --user enable --now syncme-client.service
```

To verify the daemon is running:
```bash
systemctl --user status syncme-client.service
```

To stop or restart the service:
```bash
systemctl --user stop syncme-client.service
systemctl --user restart syncme-client.service
```

### 3. View Daemon Logs

Inspect and follow daemon logs in real time using `journalctl`:
```bash
journalctl --user -u syncme-client.service -f
```

### 4. After a Rejected Key (Exit Code 78)

When the server answers 401 (for example the key was revoked), the daemon prints `Error: the server rejected the API key ...` and exits with code 78. `RestartPreventExitStatus=78` keeps systemd from restarting it, so the unit stays stopped (`systemctl --user status syncme-client` shows `failed` with status 78). Get a new key from the server operator, put it in `api_key` in the config file, and start the service again:

```bash
systemctl --user restart syncme-client
```

### 5. Enable Lingering (Keep Running After Logout)

By default, systemd user services stop when you log out of all active user sessions. To allow the service to start automatically on system boot and continue running unattended in the background:

```bash
loginctl enable-linger $USER
```

### Upgrading an existing install

The unit file in `systemd/` changes between releases (for example `RestartPreventExitStatus=78`, which keeps systemd from restarting the daemon after a rejected key). If you installed it with **Option B (copy)**, your copy in `~/.config/systemd/user/` is a snapshot and does not change when you update the repository, so `RestartPreventExitStatus=78` is **not active** until you copy the unit again (or link it, Option A), reload systemd and restart the service:

```bash
cp systemd/syncme-client.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user restart syncme-client.service
```

With **Option A (symbolic link)** the file is already current, but `systemctl --user daemon-reload` and a restart are still needed before systemd uses the new version. `systemctl --user show -p RestartPreventExitStatus syncme-client.service` shows the value systemd has loaded (`systemctl --user cat` prints the file on disk, which may be newer than what is loaded).

---

## Development & Testing

SyncMe-Client uses `pytest` and `pytest-asyncio`.

Run tests:
```bash
uv run pytest
# or with active virtual environment:
pytest tests/ -v
```
