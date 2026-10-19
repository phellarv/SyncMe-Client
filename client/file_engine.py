import asyncio
import os
import time
from pathlib import Path
import httpx

from client.db import LocalDB
from client.api import APIClient
from client.config import ConfigError, ServerConfig
from client.errors import is_auth_error
from client.archive import archive_file
from client.watcher import Watcher
from client.notifications import handle_conflict_notification
from client.base_engine import BaseEngine, get_file_sha256, prepare_upload_payload


class FileGroupEngine(BaseEngine):
    def __init__(self, workspace_id, files, db, api, server_config):
        seen_remote = set()
        for f in files:
            r_path = getattr(f, "remote_path", None)
            if r_path is not None:
                if r_path in seen_remote:
                    raise ConfigError(f"Duplicate remote_path '{r_path}' in workspace '{workspace_id}'")
                seen_remote.add(r_path)

        poll_interval = getattr(server_config, 'poll_interval_seconds', 300) if server_config else 300
        super().__init__(
            workspace_id=workspace_id,
            db=db,
            api=api,
            poll_interval_seconds=poll_interval,
        )
        self.files = files
        self.server_config = server_config

    def _get_f_conf(self, path):
        for f in self.files:
            if f.remote_path == path:
                return f
        return None

    def archive_local_file(self, full_path: Path, rel_path: str):
        archive_file(full_path.parent, full_path.name)

    async def reconcile(self):
        if not self.db.is_initialized():
            raise RuntimeError(f"SyncMe is not initialized. Please run 'syncme-client init' first. ({self.workspace_id})")
            
        async with self._lock:
            self.prune_missing_conflicts()
            remote_state = await self.api.get_state(self.db.get_last_server_revision())
            latest_rev = remote_state.get('latest_revision', remote_state.get('last_revision', 0))

            # An entry whose fetch failed (network or HTTP error, hash mismatch, local I/O error)
            # is counted and the stored revision is then left alone, so the next reconcile
            # fetches it again. Deliberate skips are not counted.
            retryable_skips = 0
            for file_info in remote_state.get('files', []):
                if not (isinstance(file_info, dict) and isinstance(file_info.get('path'), str) and 'revision' in file_info):
                    continue
                path = file_info['path']
                f_conf = self._get_f_conf(path)
                if not f_conf:
                    continue

                try:
                    if await self._reconcile_remote_entry(path, f_conf.local_path, file_info, f_conf.replace_symlinks):
                        retryable_skips += 1
                except Exception as e:
                    if is_auth_error(e):
                        raise
                    print(f"[{self.workspace_id}] Warning: skipping server entry {path!r}: {e!r}", flush=True)
                    retryable_skips += 1

            self.finalize_server_revision(latest_rev, retryable_skips)

            # 2. Process local deletions
            for f_conf in self.files:
                path = f_conf.remote_path
                full_path = f_conf.local_path
                if not full_path.exists() and not full_path.is_symlink():
                    local_file = self.db.get_file(path)
                    if local_file and not local_file.get('is_deleted', False):
                        await self._delete_remote_file(path, local_file['server_revision'])

            # 3. Process local changes
            await self._sync_local_changes()

    async def _sync_local_changes(self):
        for f_conf in self.files:
            await self._upload_local_file_if_changed(f_conf.remote_path, f_conf.local_path, f_conf.replace_symlinks)

    async def initial_sync(self, conflict_callback=None):
        if conflict_callback is None:
            conflict_callback = lambda rel_path, s_file, l_file: "ask"

        async with self._lock:
            remote_state = await self.api.get_state(0)
            remote_files = {f['path']: f for f in remote_state.get('files', []) if not f.get('is_deleted', False)}
            latest_rev = remote_state.get('latest_revision', remote_state.get('last_revision', 0))

            local_files = {}
            for f_conf in self.files:
                path = f_conf.remote_path
                full_path = f_conf.local_path
                if full_path.is_symlink():
                    if not f_conf.replace_symlinks:
                        continue
                    if not full_path.is_file():
                        continue
                elif not full_path.is_file():
                    continue
                try:
                    stat = full_path.stat()
                except (FileNotFoundError, OSError):
                    continue
                local_files[path] = {
                    "path": full_path,
                    "stat": stat,
                    "sha256": get_file_sha256(full_path)
                }

            summary = {"uploaded": 0, "downloaded": 0, "conflicts": 0, "unchanged": 0, "failed": 0}
            
            for f_conf in self.files:
                path = f_conf.remote_path
                full_path = f_conf.local_path
                await self._initial_sync_entry(
                    rel_path=path,
                    resolve_full_path=lambda _p, fp=full_path: fp,
                    in_local=path in local_files,
                    in_remote=path in remote_files,
                    local_files=local_files,
                    remote_files=remote_files,
                    replace_symlinks=f_conf.replace_symlinks,
                    conflict_callback=conflict_callback,
                    summary=summary,
                )

            self.db.set_initialized(True)
            # Only a clean run may move the stored revision forward. With failures it stays
            # where it was, so the next reconcile fetches the same entries again.
            if summary["failed"] == 0:
                self.db.set_last_server_revision(latest_rev)
            return summary

    async def _watch_loop(self):
        watchers = []
        try:
            # Watch each parent directory once and not recursively: a file such as ~/.bashrc would
            # otherwise put an inotify watch on every directory under ~ and exhaust the watch limit.
            replace_symlinks_by_parent = {}
            for f in self.files:
                parent = f.local_path.parent
                replace_symlinks_by_parent[parent] = replace_symlinks_by_parent.get(parent, False) or f.replace_symlinks
            for parent, replace_symlinks in replace_symlinks_by_parent.items():
                try:
                    parent.mkdir(parents=True, exist_ok=True)
                except OSError as e:
                    print(f"[{self.workspace_id}] Warning: could not create directory {parent}: {e}", flush=True)
                    continue
                try:
                    w = Watcher(parent, self.server_config.debounce_seconds, replace_symlinks, recursive=False)
                    w.start()
                    watchers.append(w)
                except Exception as e:
                    print(f"[{self.workspace_id}] Warning: failed to watch {parent}: {e}", flush=True)
            
            async def pull_events(w):
                async for batch in w.get_batches():
                    should_reconcile = False
                    w_dir = getattr(w, "sync_dir", None)
                    for rel_path, _ in batch:
                        if ".conflict." in rel_path or Path(rel_path).name.startswith(".syncme-tmp-"):
                            continue
                        for f in self.files:
                            if w_dir is not None:
                                if (Path(w_dir) / Path(rel_path).name) == f.local_path:
                                    should_reconcile = True
                                    break
                            elif f.local_path.name == rel_path or f.local_path.name == Path(rel_path).name:
                                should_reconcile = True
                                break
                        if should_reconcile:
                            break
                    if should_reconcile:
                        await self._reconcile_safely()

            await asyncio.gather(*[pull_events(w) for w in watchers])
        finally:
            for w in watchers:
                w.stop()
