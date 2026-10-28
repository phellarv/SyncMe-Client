from client.conflicts import format_inline_diff
import json
import click
import asyncio
from pathlib import Path
import sys

from client.config import settings, save_default_config, load_config, ConfigError, check_server_config, warn_if_config_loose, get_sync_targets, get_sync_targets_toml
from client.db import LocalDB
from client.api import APIClient
from client.engine import SyncEngine
from client.archive import list_archive, restore_file
from client.errors import EXIT_AUTH, is_auth_error

def _get_engines(directory=None):
    engines = []
    for d_conf in settings.directories:
        if directory and d_conf.name != directory:
            continue
        sync_dir = d_conf.local_path
        sync_dir.mkdir(parents=True, exist_ok=True)
        db_path = sync_dir / ".syncme" / "local_state.db"
        db = LocalDB(db_path)
        api = APIClient(settings.SERVER_URL, settings.API_KEY, settings.CLIENT_ID, workspace_id=d_conf.name, timeout=settings.REQUEST_TIMEOUT)
        engines.append((d_conf, SyncEngine(sync_dir, db, api, settings.DEBOUNCE_SECONDS, settings.POLL_INTERVAL_SECONDS, replace_symlinks=d_conf.replace_symlinks, exceptions=d_conf.exceptions)))

    from collections import defaultdict
    files_by_workspace = defaultdict(list)
    for f_conf in settings.files:
        files_by_workspace[f_conf.workspace].append(f_conf)
        
    for workspace, files in files_by_workspace.items():
        if directory and workspace != directory:
            continue
        from client.file_engine import FileGroupEngine
        from client.config import get_central_state_dir
        
        class DummyConf:
            def __init__(self, name, conflict_strategy, local_path):
                self.name = name
                self.conflict_strategy = conflict_strategy
                self.local_path = local_path
                self.replace_symlinks = False
                self.exceptions = []
                
        d_conf = DummyConf(workspace, files[0].conflict_strategy, get_central_state_dir())
        
        db_path = get_central_state_dir() / f"{workspace}.db"
        db = LocalDB(db_path)
        api = APIClient(settings.SERVER_URL, settings.API_KEY, settings.CLIENT_ID, workspace_id=workspace, timeout=settings.REQUEST_TIMEOUT)
        
        engines.append((d_conf, FileGroupEngine(workspace, files, db, api, settings.server)))

    return engines

def _run_guarded(coro):
    """Like asyncio.run, but a rejected key (HTTP 401) ends with one clear message and exit code 78."""
    try:
        return asyncio.run(coro)
    except Exception as e:
        if is_auth_error(e):
            click.secho("Error: the server rejected the API key (HTTP 401). The key may have been revoked: "
                        f"get a new one from the server operator and update api_key in the config file.", fg="red", bold=True)
            sys.exit(EXIT_AUTH)
        raise

def _require_server_config():
    warn_if_config_loose()  # before the checks, so the warning also shows when they fail
    try:
        check_server_config(settings.server)
    except ConfigError as e:
        click.secho(f"Error: {e}", fg="red", bold=True)
        sys.exit(1)

def check_initialized(engine, name):
    if not engine.db.is_initialized():
        click.secho(f"Error: SyncMe is not initialized for directory '{name}'. Please run 'syncme-client init' first.", fg="red", bold=True)
        sys.exit(1)

@click.group()
def cli():
    """SyncMe Client CLI"""
    pass

@cli.command()
@click.option('--directory', '-d', help="Specific directory to initialize")
@click.option('--conflict-strategy', type=click.Choice(['ask', 'server', 'local', 'both']), default=None)
def init(directory, conflict_strategy):
    """Initialize SyncMe directory and perform initial sync"""
    save_default_config()
    _require_server_config()

    engines = _get_engines(directory)
    if not engines:
        click.echo(f"Directory '{directory}' not found in config.")
        return

    # Set by S/L at the prompt; applies to every remaining conflict in this run, across directories.
    apply_to_all = None

    async def _init_engine(d_conf, engine):
        if engine.db.is_initialized():
            if not click.confirm(f"[{d_conf.name}] SyncMe is already initialized. Re-run initial sync?"):
                return

        strategy = conflict_strategy or d_conf.conflict_strategy or "ask"

        def conflict_callback(rel_path, server_file, local_file):
            nonlocal apply_to_all
            if apply_to_all:
                click.echo(f"[{d_conf.name}] Conflict: {rel_path} -> {'server' if apply_to_all == 's' else 'local'} wins (applied to all)")
                return apply_to_all
            click.echo(f"[{d_conf.name}] Conflict detected for: {rel_path}")
            click.echo(f"  Local:  size={local_file.get('size')}, mtime={local_file.get('mtime')}")
            click.echo(f"  Server: size={server_file.get('size')}, mtime={server_file.get('mtime')}")
            default_choice = None
            if strategy != 'ask':
                default_choice = strategy[0]
            
            choice = click.prompt("Choose action: [s]erver wins, [l]ocal wins, [b]oth (keep local as .conflict), "
                                  "[S]erver wins for all remaining, [L]ocal wins for all remaining",
                                  type=click.Choice(['s', 'l', 'b', 'S', 'L']), default=default_choice)
            if choice in ('S', 'L'):
                choice = apply_to_all = choice.lower()
            return choice

        try:
            summary = await engine.initial_sync(conflict_callback)
            click.echo(f"[{d_conf.name}] Initial sync complete:")
            click.echo(f"  Uploaded:   {summary['uploaded']}")
            click.echo(f"  Downloaded: {summary['downloaded']}")
            click.echo(f"  Conflicts:  {summary['conflicts']}")
            click.echo(f"  Unchanged:  {summary['unchanged']}")
            click.echo(f"  Failed:     {summary.get('failed', 0)}")
        finally:
            await engine.api.close()

    for d_conf, engine in engines:
        _run_guarded(_init_engine(d_conf, engine))

@cli.command()
@click.option('--directory', '-d', help="Specific directory to run")
def run(directory):
    """Run daemon (watcher + SSE + periodic scan)"""
    _require_server_config()
    engines = _get_engines(directory)
    if not engines:
        click.echo(f"Directory '{directory}' not found in config.")
        return
        
    for d_conf, engine in engines:
        check_initialized(engine, d_conf.name)
        
    async def _run_all():
        try:
            await asyncio.gather(*[engine.run() for _, engine in engines])
        finally:
            for _, engine in engines:
                await engine.api.close()

    _run_guarded(_run_all())

@cli.command()
@click.option('--directory', '-d', help="Specific directory to sync")
def sync(directory):
    """Run one-time sync"""
    _require_server_config()
    engines = _get_engines(directory)
    if not engines:
        click.echo(f"Directory '{directory}' not found in config.")
        return

    async def _sync_engine(d_conf, engine):
        check_initialized(engine, d_conf.name)
        try:
            await engine.reconcile()
            click.echo(f"[{d_conf.name}] Sync complete")
        finally:
            await engine.api.close()

    for d_conf, engine in engines:
        _run_guarded(_sync_engine(d_conf, engine))

@cli.command()
@click.option('--directory', '-d', help="Specific directory to check status")
def status(directory):
    """Print sync status"""
    engines = _get_engines(directory)
    for d_conf, engine in engines:
        files = engine.db.get_all_files()
        click.echo(f"[{d_conf.name}] Tracking {len(files)} files.")

def _path_status(path: Path) -> str:
    try:
        return "exists" if path.exists() else "does not exist"
    except OSError:
        return "inaccessible"

@cli.command(name="targets")
@click.option("--json", "as_json", is_flag=True, help="Output targets as JSON")
@click.option("--toml", "as_toml", is_flag=True, help="Output targets as TOML (suitable for config.toml)")
def targets(as_json: bool, as_toml: bool):
    """List all configured sync targets (directories and single files)."""
    if as_json and as_toml:
        raise click.UsageError("Cannot specify both --json and --toml")

    try:
        cfg = load_config()
    except ConfigError as e:
        click.secho(f"Error: {e}", fg="red", bold=True)
        sys.exit(1)

    if as_toml:
        click.echo(get_sync_targets_toml(cfg), nl=False)
        return

    sync_targets = get_sync_targets(cfg)

    if as_json:
        payload = {
            "directories": [d.model_dump(mode="json") for d in sync_targets["directories"]],
            "files": [f.model_dump(mode="json") for f in sync_targets["files"]],
        }
        click.echo(json.dumps(payload, indent=2))
        return

    click.echo("Directories:")
    if not sync_targets["directories"]:
        click.echo("  (none)")
    else:
        for d in sync_targets["directories"]:
            state = _path_status(d.local_path)
            click.echo(f"  - Name: {d.name}")
            click.echo(f"    Path: {d.local_path} ({state})")
            click.echo(f"    Conflict strategy: {d.conflict_strategy}")
            click.echo(f"    Replace symlinks: {d.replace_symlinks}")
            exceptions_str = ", ".join(d.exceptions) if d.exceptions else "none"
            click.echo(f"    Exceptions: {exceptions_str}")

    click.echo("\nFiles:")
    if not sync_targets["files"]:
        click.echo("  (none)")
    else:
        for f in sync_targets["files"]:
            state = _path_status(f.local_path)
            click.echo(f"  - Workspace: {f.workspace}")
            click.echo(f"    Path: {f.local_path} ({state})")
            click.echo(f"    Remote path: {f.remote_path}")
            click.echo(f"    Conflict strategy: {f.conflict_strategy}")
            click.echo(f"    Replace symlinks: {f.replace_symlinks}")

@cli.group()
def archive():
    """Archive commands"""
    pass

def _get_archive_dirs(d_conf, engine) -> list[Path]:
    files = getattr(engine, "files", None)
    if isinstance(files, (list, tuple)) and files:
        dirs = []
        for f_conf in files:
            parent = Path(f_conf.local_path).parent
            if parent not in dirs:
                dirs.append(parent)
        return dirs
    return [d_conf.local_path]

@archive.command(name="list")
@click.option('--directory', '-d', help="Specific directory to list archive")
def archive_list(directory):
    """List archived files"""
    engines = _get_engines(directory)
    for d_conf, engine in engines:
        archives = []
        for base_dir in _get_archive_dirs(d_conf, engine):
            archives.extend(list_archive(base_dir))
        click.echo(f"[{d_conf.name}] {len(archives)} archives found:")
        for a in archives:
            click.echo(f"  {a['name']} (size: {a['size']})")

@archive.command(name="restore")
@click.argument("archive_name")
@click.argument("target_path", required=False)
@click.option('--directory', '-d', help="Specific directory to restore from")
def archive_restore(archive_name, target_path, directory):
    """Restore file"""
    engines = _get_engines(directory)
    if not engines:
        click.echo(f"Directory '{directory}' not found in config.")
        return
        
    if len(engines) > 1:
        click.echo("Multiple directories found. Please specify --directory for restore.")
        return
        
    d_conf, engine = engines[0]
    
    if target_path is None:
        target_path = archive_name.rsplit("_", 1)[0].replace("__", "/")
    restored = False
    last_err = None
    for base_dir in _get_archive_dirs(d_conf, engine):
        try:
            restore_file(base_dir, archive_name, target_path)
            restored = True
            break
        except FileNotFoundError as e:
            last_err = e
    if not restored and last_err is not None:
        raise last_err
    click.echo(f"Restored {archive_name} to {target_path} in {d_conf.name}")


def get_local_dbs():
    dbs = []
    for d_conf in settings.directories:
        sync_dir = d_conf.local_path
        db_path = sync_dir / ".syncme" / "local_state.db"
        if db_path.exists():
            dbs.append((d_conf.name, LocalDB(db_path)))
            
    from collections import defaultdict
    files_by_workspace = defaultdict(list)
    for f_conf in settings.files:
        files_by_workspace[f_conf.workspace].append(f_conf)
        
    for workspace in files_by_workspace:
        from client.config import get_central_state_dir
        db_path = get_central_state_dir() / f"{workspace}.db"
        if db_path.exists():
            dbs.append((workspace, LocalDB(db_path)))
            
    return dbs

def get_conflict_resolver(workspace: str = None, db=None):
    from client.conflicts import ConflictResolver
    from client.api import APIClient
    if not db:
        for item in get_local_dbs():
            w, d = item if isinstance(item, tuple) else ("default", item)
            if db is None and (w == workspace or not workspace):
                db = d
                workspace = w
            else:
                try:
                    d.close()
                except Exception:
                    pass
    if not db:
        return None
    
    api = APIClient(settings.SERVER_URL, settings.API_KEY, settings.CLIENT_ID, workspace_id=workspace, timeout=settings.REQUEST_TIMEOUT)
    return ConflictResolver(db, api=api)

@cli.group()
def conflicts():
    """Manage conflicts"""
    pass

def _close_dbs(dbs):
    for item in dbs:
        db = item[1] if isinstance(item, tuple) else item
        try:
            db.close()
        except Exception:
            pass

@conflicts.command(name="list")
def conflicts_list():
    """List all pending conflicts"""
    dbs = get_local_dbs()
    try:
        found = False
        for item in dbs:
            workspace, db = item if isinstance(item, tuple) else ("default", item)
            pending = db.get_pending_conflicts(workspace)
            if pending:
                found = True
                click.echo(f"Workspace: {workspace}")
                for c in pending:
                    click.echo(f"  [{c['id']}] {c['rel_path']}")
        if not found:
            click.echo("No pending conflicts found.")
    finally:
        _close_dbs(dbs)

@conflicts.command(name="count")
@click.option('--format', type=click.Choice(['plain', 'waybar', 'json']), default='plain')
def conflicts_count(format):
    """Count pending conflicts"""
    import json
    dbs = get_local_dbs()
    try:
        count = 0
        all_conflicts = []
        for item in dbs:
            workspace, db = item if isinstance(item, tuple) else ("default", item)
            count += db.count_pending_conflicts(workspace)
            if format != 'plain':
                all_conflicts.extend(db.get_pending_conflicts(workspace))
            
        if format == 'plain':
            click.echo(str(count))
        elif format == 'waybar':
            res = {
                "text": f"⚠️ {count}" if count > 0 else "✔",
                "tooltip": f"{count} conflicts pending" if count > 0 else "All synced",
                "class": "warning" if count > 0 else "synced"
            }
            click.echo(json.dumps(res))
        elif format == 'json':
            res = {
                "count": count,
                "conflicts": all_conflicts
            }
            click.echo(json.dumps(res))
    finally:
        _close_dbs(dbs)

@conflicts.command(name="resolve")
def conflicts_resolve():
    """Interactively resolve conflicts"""
    dbs = get_local_dbs()

    async def _async_resolve():
        found = False
        opened_resolvers = []
        try:
            for item in dbs:
                workspace, db = item if isinstance(item, tuple) else ("default", item)
                resolver = get_conflict_resolver(workspace, db)
                if not resolver:
                    continue
                
                opened_resolvers.append(resolver)
                    
                pending = db.get_pending_conflicts(workspace)
                if not pending:
                    continue
                    
                found = True
                for c in pending:
                    click.echo(f"\nConflict in workspace {workspace}: {c['rel_path']}")
                    diff_str = format_inline_diff(c.get('server_file'), c.get('conflict_file'))
                    click.echo(diff_str)
                    
                    choice = click.prompt("Choose action: [s]erver wins, [l]ocal wins, [m]eld, [d]ismiss, [n]ext, [q]uit", type=click.Choice(['s', 'l', 'm', 'd', 'n', 'q']))
                    
                    if choice == 'q':
                        return found
                    elif choice == 'n':
                        continue
                    elif choice == 'l':
                        action = 'local'
                    elif choice == 's':
                        action = 'server'
                    elif choice == 'm':
                        action = 'meld'
                    elif choice == 'd':
                        action = 'dismiss'
                        
                    try:
                        res = resolver.resolve_conflict(c['id'], action)
                        if asyncio.iscoroutine(res):
                            success = await res
                        else:
                            success = res
                        if not success:
                            click.echo("Failed to resolve conflict.")
                    except Exception as e:
                        click.echo(f"Error during resolution: {e}")
            return found
        finally:
            for resolver in opened_resolvers:
                if hasattr(resolver, 'api') and hasattr(resolver.api, 'close'):
                    res2 = resolver.api.close()
                    if asyncio.iscoroutine(res2):
                        await res2

    try:
        found = asyncio.run(_async_resolve())
        if not found:
            click.echo("No pending conflicts found.")
    finally:
        _close_dbs(dbs)

def _find_file_in_engines(engines, file_path):
    file_path_obj = Path(file_path).resolve()
    # 1. Exact match against standalone configured files first
    for d_conf, engine in engines:
        files = getattr(engine, "files", None)
        if isinstance(files, (list, tuple)):
            for f_conf in files:
                if Path(f_conf.local_path).resolve() == file_path_obj:
                    return engine, str(f_conf.remote_path).replace("\\", "/"), Path(f_conf.local_path).resolve()
    # 2. Directory prefix match for directory sync engines
    for d_conf, engine in engines:
        files = getattr(engine, "files", None)
        if isinstance(files, (list, tuple)):
            continue
        local_path = Path(d_conf.local_path).resolve()
        try:
            rel_path = file_path_obj.relative_to(local_path)
            resolved_path = str(rel_path).replace("\\", "/")
            return engine, resolved_path, file_path_obj
        except ValueError:
            pass
    return None, None, None

@cli.command()
@click.argument("file_path")
@click.option("--limit", "-l", default=50, help="Maximum number of revisions to list")
@click.option("--directory", "-d", help="Specific directory to operate on")
def history(file_path: str, limit: int, directory: str):
    """View version history for a file"""
    _require_server_config()
    engines = _get_engines(directory)
    if not engines:
        click.echo(f"Directory '{directory}' not found in config.")
        return
        
    engine_match, resolved_path, _ = _find_file_in_engines(engines, file_path)
    if not engine_match:
        click.echo(f"File {file_path} is not in any configured sync directory.")
        sys.exit(1)
        
    async def _get_history(engine, path, limit):
        try:
            result = await engine.api.get_history(path, limit=limit)
            
            click.echo(f"History for {path}:")
            click.echo(f"{'Revision':<10} | {'Timestamp':<25} | {'Size':<10} | {'Client':<20} | {'Deleted':<10}")
            click.echo("-" * 85)
            
            import datetime
            for rev in result.get("history", []):
                deleted_str = "Yes" if rev.get("is_deleted") else "No"
                client_str = rev.get("client_name") or "Unknown"
                created_at = rev.get("created_at")
                ts_str = datetime.datetime.fromtimestamp(created_at).isoformat(sep=" ", timespec="seconds") if created_at else "N/A"
                
                click.echo(
                    f"{rev.get('revision'):<10} | "
                    f"{ts_str:<25} | "
                    f"{rev.get('size', 0):<10} | "
                    f"{client_str:<20} | "
                    f"{deleted_str:<10}"
                )
        except Exception as e:
            click.echo(f"Error fetching history: {e}")
            sys.exit(1)
        finally:
            if hasattr(engine, "db") and hasattr(engine.db, "close"):
                try:
                    engine.db.close()
                except Exception:
                    pass
            await engine.api.close()
            
    _run_guarded(_get_history(engine_match, resolved_path, limit))

@cli.command()
@click.argument("file_path")
@click.option("--revision", "-r", type=int, required=True, help="Revision to restore")
@click.option("--directory", "-d", help="Specific directory to operate on")
def restore(file_path: str, revision: int, directory: str):
    """Restore a file to a specific revision"""
    _require_server_config()
    engines = _get_engines(directory)
    if not engines:
        click.echo(f"Directory '{directory}' not found in config.")
        return
        
    engine_match, resolved_path, target_local_path = _find_file_in_engines(engines, file_path)
    if not engine_match:
        click.echo(f"File {file_path} is not in any configured sync directory.")
        sys.exit(1)
        
    async def _restore(engine, path, rev, local_target):
        new_rev = None
        try:
            local_file = engine.db.get_file(path)
            expected_revision = local_file.get("server_revision") if local_file else None
            
            result = await engine.api.restore_history(path, rev, expected_revision=expected_revision)
            new_rev = result.get("revision") if result.get("revision") is not None else result.get("new_revision")
            expected_sha256 = result.get("sha256")
            
            content = await engine.api.download_file(path)
            from client.pathsafe import sha256_matches
            if not sha256_matches(content, expected_sha256):
                raise ValueError(f"Hash mismatch for restored file {path}")
            
            import tempfile
            import os
            import hashlib
            import stat as stat_module
            
            old_mode = None
            if local_target.exists():
                try:
                    old_mode = stat_module.S_IMODE(local_target.stat().st_mode)
                except OSError:
                    pass
            
            local_target.parent.mkdir(parents=True, exist_ok=True)
            tmp = tempfile.NamedTemporaryFile(dir=local_target.parent, prefix=".syncme-tmp-", delete=False)
            tmp_path = Path(tmp.name)
            try:
                with tmp:
                    tmp.write(content)
                    tmp.flush()
                    os.fsync(tmp.fileno())
                if old_mode is not None:
                    try:
                        os.chmod(tmp_path, old_mode)
                    except OSError:
                        pass
                tmp_path.replace(local_target)
            except Exception:
                tmp_path.unlink(missing_ok=True)
                raise
                
            sha = hashlib.sha256(content).hexdigest()
            stat = local_target.stat()
            engine.db.upsert_file(path, sha, stat.st_size, stat.st_mtime, new_rev, False)
            
            click.echo(f"Successfully restored {path} to revision {rev}. Server created new revision {new_rev}.")
        except Exception as e:
            if new_rev is not None:
                click.echo(f"Restored to revision {new_rev} on server, but local update failed: {e}. Run sync to update.")
            else:
                click.echo(f"Error restoring file: {e}")
            sys.exit(1)
        finally:
            if hasattr(engine, "db") and hasattr(engine.db, "close"):
                try:
                    engine.db.close()
                except Exception:
                    pass
            await engine.api.close()
            
    _run_guarded(_restore(engine_match, resolved_path, revision, target_local_path))

if __name__ == "__main__":
    cli()
