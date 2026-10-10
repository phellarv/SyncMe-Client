from client.conflicts import format_inline_diff
import click
import asyncio
from pathlib import Path
import sys

from client.config import settings, save_default_config, load_config, ConfigError, check_server_config, warn_if_config_loose
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
        engines.append((d_conf, SyncEngine(sync_dir, db, api, settings.DEBOUNCE_SECONDS, settings.POLL_INTERVAL_SECONDS, replace_symlinks=d_conf.replace_symlinks)))

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

    async def _init_engine(d_conf, engine):
        if engine.db.is_initialized():
            if not click.confirm(f"[{d_conf.name}] SyncMe is already initialized. Re-run initial sync?"):
                return

        strategy = conflict_strategy or d_conf.conflict_strategy or "ask"

        def conflict_callback(rel_path, server_file, local_file):
            click.echo(f"[{d_conf.name}] Conflict detected for: {rel_path}")
            click.echo(f"  Local:  size={local_file.get('size')}, mtime={local_file.get('mtime')}")
            click.echo(f"  Server: size={server_file.get('size')}, mtime={server_file.get('mtime')}")
            default_choice = None
            if strategy != 'ask':
                default_choice = strategy[0]
            
            choice = click.prompt("Choose action: [s]erver wins, [l]ocal wins, [b]oth (keep local as .conflict)", type=click.Choice(['s', 'l', 'b']), default=default_choice)
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

@cli.group()
def archive():
    """Archive commands"""
    pass

@archive.command(name="list")
@click.option('--directory', '-d', help="Specific directory to list archive")
def archive_list(directory):
    """List archived files"""
    engines = _get_engines(directory)
    for d_conf, engine in engines:
        archives = list_archive(d_conf.local_path)
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
        target_path = archive_name.split("_")[0].replace("__", "/")
    restore_file(d_conf.local_path, archive_name, target_path)
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
            if w == workspace or not workspace:
                db = d
                workspace = w
                break
    if not db:
        return None
    
    api = APIClient(settings.SERVER_URL, settings.API_KEY, settings.CLIENT_ID, workspace_id=workspace, timeout=settings.REQUEST_TIMEOUT)
    return ConflictResolver(db, api=api)

@cli.group()
def conflicts():
    """Manage conflicts"""
    pass

@conflicts.command(name="list")
def conflicts_list():
    """List all pending conflicts"""
    dbs = get_local_dbs()
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

@conflicts.command(name="count")
@click.option('--format', type=click.Choice(['plain', 'waybar', 'json']), default='plain')
def conflicts_count(format):
    """Count pending conflicts"""
    import json
    dbs = get_local_dbs()
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

@conflicts.command(name="resolve")
def conflicts_resolve():
    """Interactively resolve conflicts"""
    async def _async_resolve():
        dbs = get_local_dbs()
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
                    
                    choice = click.prompt("Choose action: [s]erver wins, [l]ocal wins, [n]ext, [q]uit", type=click.Choice(['s', 'l', 'n', 'q']))
                    
                    if choice == 'q':
                        return found
                    elif choice == 'n':
                        continue
                    elif choice == 'l':
                        action = 'local'
                    elif choice == 's':
                        action = 'server'
                        
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

    found = asyncio.run(_async_resolve())
    if not found:
        click.echo("No pending conflicts found.")

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
        
    engine_match = None
    resolved_path = None
    
    file_path_obj = Path(file_path).resolve()
    
    for d_conf, engine in engines:
        local_path = Path(d_conf.local_path).resolve()
        try:
            rel_path = file_path_obj.relative_to(local_path)
            engine_match = engine
            resolved_path = str(rel_path).replace("\\", "/")
            break
        except ValueError:
            pass
            
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
        
    engine_match = None
    resolved_path = None
    
    file_path_obj = Path(file_path).resolve()
    
    for d_conf, engine in engines:
        local_path = Path(d_conf.local_path).resolve()
        try:
            rel_path = file_path_obj.relative_to(local_path)
            engine_match = engine
            resolved_path = str(rel_path).replace("\\", "/")
            break
        except ValueError:
            pass
            
    if not engine_match:
        click.echo(f"File {file_path} is not in any configured sync directory.")
        sys.exit(1)
        
    async def _restore(engine, path, rev):
        try:
            local_file = engine.db.get_file(path)
            expected_revision = local_file.get("server_revision") if local_file else None
            
            result = await engine.api.restore_history(path, rev, expected_revision=expected_revision)
            new_rev = result.get("revision") or result.get("new_revision")
            click.echo(f"Successfully restored {path} to revision {rev}. Server created new revision {new_rev}.")
        except Exception as e:
            click.echo(f"Error restoring file: {e}")
            sys.exit(1)
        finally:
            await engine.api.close()
            
    _run_guarded(_restore(engine_match, resolved_path, revision))

if __name__ == "__main__":
    cli()
