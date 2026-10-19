import asyncio
import os
import time
from pathlib import Path
from typing import Sequence
import httpx

from client.config import settings
from client.errors import is_auth_error
from client.db import LocalDB
from client.api import APIClient, STREAM_THRESHOLD
from client.ignore import is_ignored
from client.archive import archive_file
from client.watcher import Watcher
from client.notifications import handle_conflict_notification
from client.pathsafe import UnsafePathError, safe_join
from client.base_engine import BaseEngine, get_file_sha256, prepare_upload_payload


class SyncEngine(BaseEngine):
    def __init__(
        self,
        sync_dir: Path,
        db: LocalDB,
        api: APIClient,
        debounce_seconds: float = 1.0,
        poll_interval_seconds: int = 300,
        replace_symlinks: bool = False,
        exceptions: Sequence[str] = (),
    ):
        workspace_id = getattr(api, "workspace_id", None) if isinstance(getattr(api, "workspace_id", None), str) else sync_dir.name
        super().__init__(
            workspace_id=workspace_id,
            db=db,
            api=api,
            poll_interval_seconds=poll_interval_seconds,
        )
        self.sync_dir = sync_dir
        self.replace_symlinks = replace_symlinks
        self.exceptions = tuple(exceptions or ())
        self.watcher = Watcher(sync_dir, debounce_seconds, replace_symlinks, exceptions=self.exceptions)

    @property
    def log_label(self) -> str:
        return self.sync_dir.name

    def is_ignored(self, rel_path: str) -> bool:
        return is_ignored(self.sync_dir, rel_path, self.exceptions)

    def archive_local_file(self, full_path: Path, rel_path: str):
        archive_file(self.sync_dir, rel_path)

    async def reconcile(self):
        if not self.db.is_initialized():
            raise RuntimeError("SyncMe is not initialized. Please run 'syncme-client init' first.")

        if not self.sync_dir.is_dir():
            print(f"[{self.sync_dir.name}] Warning: sync directory does not exist or is not a directory, aborting reconcile", flush=True)
            return

        async with self._lock:
            self.prune_missing_conflicts()
            # 1. Get remote state
            remote_state = await self.api.get_state(self.db.get_last_server_revision())
            latest_rev = remote_state.get('latest_revision', remote_state.get('last_revision', 0))
            
            # 2. Process remote changes. An entry skipped for a retryable reason (network
            # or HTTP error, hash mismatch, local I/O error, ...) is counted, and the stored
            # revision is then left alone so the next reconcile fetches it again. Permanent
            # rejections (unsafe or malformed entries, deliberate skips) are not counted:
            # retrying them would never succeed and must not hold the revision back.
            retryable_skips = 0
            for file_info in remote_state.get('files', []):
                if not (isinstance(file_info, dict) and isinstance(file_info.get('path'), str) and 'revision' in file_info):
                    # Checked up front rather than by catching KeyError/TypeError below, so a
                    # KeyError from deeper code is never mistaken for a malformed entry.
                    name = file_info.get('path') if isinstance(file_info, dict) else file_info
                    print(f"[{self.sync_dir.name}] Warning: skipping malformed server entry {name!r}", flush=True)
                    continue
                try:
                    path = file_info['path']
                    full_path = safe_join(self.sync_dir, path)
                    if self.is_ignored(path):
                        continue

                    if await self._reconcile_remote_entry(path, full_path, file_info, self.replace_symlinks):
                        retryable_skips += 1
                except UnsafePathError as e:
                    print(f"[{self.sync_dir.name}] Warning: skipping unsafe path from server: {e}", flush=True)
                except Exception as e:
                    if is_auth_error(e):
                        raise  # a rejected key is not a poisoned entry: stop, do not skip and retry
                    # One poisoned entry must not abort the whole sync. Exception rather than
                    # BaseException, so cancellation still propagates. repr() keeps a lone
                    # surrogate in the path from breaking the warning itself.
                    print(f"[{self.sync_dir.name}] Warning: skipping server entry {file_info['path']!r}: {e!r}", flush=True)
                    retryable_skips += 1

            self.finalize_server_revision(latest_rev, retryable_skips)

            # 2.5 Process local deletions
            if not self.sync_dir.is_dir():
                print(f"[{self.sync_dir.name}] Warning: sync directory does not exist or is not a directory, skipping local deletion scan", flush=True)
            else:
                active_files = [
                    f for f in self.db.get_all_files()
                    if not f['is_deleted'] and not self.is_ignored(f['path'])
                ]
                missing_files = [
                    f for f in active_files
                    if not (self.sync_dir / f['path']).exists()
                ]
                total_active = len(active_files)
                missing_count = len(missing_files)

                if total_active >= 5 and (missing_count == total_active or missing_count / total_active > 0.5):
                    print(
                        f"[{self.sync_dir.name}] Warning: mass deletion threshold exceeded "
                        f"({missing_count}/{total_active} files missing), skipping local deletion pass to prevent catastrophic data loss",
                        flush=True,
                    )
                else:
                    for local_file in missing_files:
                        rel_path = local_file['path']
                        await self._delete_remote_file(rel_path, local_file['server_revision'])

            # 3. Process local changes
            import unicodedata
            for root, dirs, files in os.walk(self.sync_dir):
                dirs[:] = [d for d in dirs if d != ".syncme" and not self.is_ignored(unicodedata.normalize("NFC", str((Path(root) / d).relative_to(self.sync_dir)).replace(os.sep, "/")) + "/")]
                    
                for file in files:
                    full_path = Path(root) / file
                    rel_path = unicodedata.normalize("NFC", str(full_path.relative_to(self.sync_dir)).replace(os.sep, "/"))
                    if self.is_ignored(rel_path):
                        continue
                    await self._upload_local_file_if_changed(rel_path, full_path, self.replace_symlinks, log_upload=True)

    async def initial_sync(self, conflict_callback=None):
        if conflict_callback is None:
            conflict_callback = lambda rel_path, s_file, l_file: "ask"

        async with self._lock:
            # 1. Get remote state
            remote_state = await self.api.get_state(0)
            remote_files = {}
            for f in remote_state.get('files', []):
                if not isinstance(f, dict):
                    print(f"[{self.sync_dir.name}] Warning: skipping malformed server entry: {f!r}", flush=True)
                    continue
                if f.get('is_deleted', False):
                    continue
                try:
                    safe_join(self.sync_dir, f.get('path'))
                except UnsafePathError as e:
                    print(f"[{self.sync_dir.name}] Warning: skipping unsafe path from server: {e}", flush=True)
                    continue
                remote_files[f['path']] = f
            latest_rev = remote_state.get('latest_revision', remote_state.get('last_revision', 0))

            # 2. Get local files
            import unicodedata
            local_files = {}
            for root, dirs, files in os.walk(self.sync_dir):
                dirs[:] = [d for d in dirs if d != ".syncme" and not self.is_ignored(unicodedata.normalize("NFC", str((Path(root) / d).relative_to(self.sync_dir)).replace(os.sep, "/")) + "/")]
                for file in files:
                    full_path = Path(root) / file
                    rel_path = unicodedata.normalize("NFC", str(full_path.relative_to(self.sync_dir)).replace(os.sep, "/"))
                    if not self.is_ignored(rel_path):
                        if full_path.is_symlink():
                            if not self.replace_symlinks:
                                continue
                            if not full_path.is_file():
                                continue
                        
                        try:
                            stat = full_path.stat()
                        except (FileNotFoundError, OSError):
                            continue
                            
                        local_files[rel_path] = {
                            "path": full_path,
                            "stat": stat,
                            "sha256": get_file_sha256(full_path)
                        }

            summary = {"uploaded": 0, "downloaded": 0, "conflicts": 0, "unchanged": 0, "failed": 0}
            
            # 3. Process each file
            all_paths = set(remote_files.keys()).union(set(local_files.keys()))
            for rel_path in all_paths:
                if self.is_ignored(rel_path):
                    continue
                await self._initial_sync_entry(
                    rel_path=rel_path,
                    resolve_full_path=lambda p: safe_join(self.sync_dir, p),
                    in_local=rel_path in local_files,
                    in_remote=rel_path in remote_files,
                    local_files=local_files,
                    remote_files=remote_files,
                    replace_symlinks=self.replace_symlinks,
                    conflict_callback=conflict_callback,
                    summary=summary,
                )
            
            self.db.set_initialized(True)
            # Only a clean run may move the stored revision forward. With failures it stays
            # where it was (0 on a fresh DB), so the next reconcile fetches the same entries
            # again and retries the files that failed.
            if summary["failed"] == 0:
                self.db.set_last_server_revision(latest_rev)
            
            return summary

    async def _watch_loop(self):
        self.watcher.start()
        try:
            async for batch in self.watcher.get_batches():
                print(f"[{self.sync_dir.name}] Local changes detected ({len(batch)} items) - reconciling...", flush=True)
                await self._reconcile_safely()
        finally:
            self.watcher.stop()
