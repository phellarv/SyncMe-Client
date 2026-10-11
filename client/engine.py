import asyncio
import hashlib
import os
import time
from pathlib import Path
from typing import Sequence
import httpx

from client.config import settings
from client.errors import describe_stream_error, is_auth_error
from client.db import LocalDB
from client.api import APIClient
from client.ignore import is_ignored
from client.archive import archive_file
from client.watcher import Watcher
from client.notifications import handle_conflict_notification
from client.pathsafe import UnsafePathError, safe_join, sha256_matches

def get_file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

class SyncEngine:
    def __init__(self, sync_dir: Path, db: LocalDB, api: APIClient, debounce_seconds: float = 1.0, poll_interval_seconds: int = 300, replace_symlinks: bool = False, exceptions: Sequence[str] = ()):
        self.sync_dir = sync_dir
        self.db = db
        self.api = api
        self.workspace_id = getattr(api, "workspace_id", None) if isinstance(getattr(api, "workspace_id", None), str) else sync_dir.name
        self.poll_interval_seconds = poll_interval_seconds
        self.replace_symlinks = replace_symlinks
        self.exceptions = tuple(exceptions or ())
        self.watcher = Watcher(sync_dir, debounce_seconds, replace_symlinks, exceptions=self.exceptions)
        self._lock = asyncio.Lock()
        self._tasks: set = set()
        self.sse_initial_delay = 1.0   # event stream reconnect backoff (seconds)
        self.sse_max_delay = 60.0
        self.sse_healthy_seconds = 30.0   # a stream open at least this long resets the backoff
        self.sse_clock = time.monotonic   # injectable for tests

    def is_ignored(self, rel_path: str) -> bool:
        return is_ignored(self.sync_dir, rel_path, self.exceptions)

    async def _fetch_verified(self, path: str, expected_sha256: str | None):
        content = await self.api.download_file(path)
        if not sha256_matches(content, expected_sha256):
            print(f"[{self.sync_dir.name}] Warning: hash mismatch for {path}, not written", flush=True)
            return None
        return content

    async def reconcile(self):
        if not self.db.is_initialized():
            raise RuntimeError("SyncMe is not initialized. Please run 'syncme-client init' first.")

        if not self.sync_dir.is_dir():
            print(f"[{self.sync_dir.name}] Warning: sync directory does not exist or is not a directory, aborting reconcile", flush=True)
            return

        async with self._lock:
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

                    remote_rev = file_info['revision']
                    is_deleted = file_info.get('is_deleted', False)
                    local_file = self.db.get_file(path)

                    if is_deleted:
                        # A deletion is replayed whenever a retryable skip keeps the stored
                        # revision back, so it must never destroy anything the user has made
                        # since: only a tracked file that is unchanged since the last sync
                        # (sha256 is the final decision) is archived and removed.
                        if not full_path.exists():
                            self.db.delete_file(path)
                        elif local_file and local_file['server_revision'] >= remote_rev:
                            # Stale: the local copy is at least as new as this deletion.
                            continue
                        elif local_file is None:
                            print(f"[{self.sync_dir.name}] Warning: not deleting {path!r}: the server deleted it, but the local file is not tracked here (kept as is)", flush=True)
                        elif get_file_sha256(full_path) != local_file['sha256']:
                            # The row stays, so the local-changes step below still sees the edit.
                            print(f"[{self.sync_dir.name}] Warning: not deleting {path!r}: the server deleted it, but the local file changed since the last sync (kept as is)", flush=True)
                        else:
                            archive_file(self.sync_dir, path)
                            full_path.unlink()
                            self.db.delete_file(path)
                    else:
                        if local_file and full_path.exists():
                            if local_file['server_revision'] >= remote_rev:
                                continue
                            if file_info.get('sha256') and local_file['sha256'] == file_info['sha256']:
                                self.db.upsert_file(path, local_file['sha256'], local_file['size'], local_file['mtime'], remote_rev, False)
                                continue

                        # A regular local file the DB has no record of (for instance after an
                        # initial_sync that failed and so left the stored revision unchanged)
                        # is user data: never overwrite it silently. A symlink is not, so it
                        # keeps following replace_symlinks below.
                        untracked_file = local_file is None and full_path.is_file() and not full_path.is_symlink()
                        if untracked_file:
                            local_sha = get_file_sha256(full_path)
                            if file_info.get('sha256') and local_sha == file_info['sha256']:
                                # Already identical to the server version: adopt it, no download.
                                stat = full_path.stat()
                                self.db.upsert_file(path, local_sha, stat.st_size, stat.st_mtime, remote_rev, False)
                                continue

                        # Decide whether this is a conflict, but do not touch the local
                        # tree yet: the server content must be fetched and verified first.
                        is_conflict = untracked_file  # differing, or unprovable, content
                        stat_mtime = None
                        if full_path.is_file() and not full_path.is_symlink():
                            try:
                                stat_mtime = full_path.stat().st_mtime
                            except (FileNotFoundError, OSError):
                                stat_mtime = None

                        if local_file and local_file['server_revision'] < remote_rev and full_path.exists():
                            # Check for conflict: local file modified since last sync
                            mtime_check = stat_mtime if stat_mtime is not None else local_file['mtime']
                            is_conflict = local_file['mtime'] < mtime_check

                        if not is_conflict and full_path.is_symlink() and not self.replace_symlinks:
                            continue

                        conflict_path = None
                        # Download from server. Only once the content is fetched and verified
                        # may the local file be moved aside or the local symlink be replaced;
                        # on a mismatch or a network error nothing local changes.
                        try:
                            content = await self._fetch_verified(path, file_info.get('sha256'))
                            if content is None:
                                retryable_skips += 1
                                continue

                            full_path.parent.mkdir(parents=True, exist_ok=True)
                            old_mode = None
                            if full_path.exists():
                                try:
                                    import stat as stat_module
                                    old_mode = stat_module.S_IMODE(full_path.stat().st_mode)
                                except OSError:
                                    pass

                            import tempfile
                            tmp = tempfile.NamedTemporaryFile(dir=full_path.parent, prefix=".syncme-tmp-", delete=False)
                            tmp_file = Path(tmp.name)
                            try:
                                with tmp:
                                    tmp.write(content)
                                    tmp.flush()
                                    os.fsync(tmp.fileno())

                                if old_mode is not None:
                                    try:
                                        os.chmod(tmp_file, old_mode)
                                    except OSError:
                                        pass

                                if full_path.is_symlink():
                                    if not is_conflict:
                                        full_path.unlink()
                                    else:
                                        ts = int(time.time())
                                        conflict_name = f"{full_path.stem}.conflict.{ts}{full_path.suffix}"
                                        conflict_path = full_path.parent / conflict_name
                                        cnt = 0
                                        while conflict_path.exists():
                                            cnt += 1
                                            conflict_path = full_path.parent / f"{full_path.stem}.conflict.{ts}_{cnt}{full_path.suffix}"
                                        full_path.rename(conflict_path)
                                elif full_path.is_file():
                                    if not is_conflict:
                                        try:
                                            cur_stat = full_path.stat()
                                            mtime_changed = stat_mtime is not None and cur_stat.st_mtime > stat_mtime
                                            hash_changed = local_file and get_file_sha256(full_path) != local_file['sha256']
                                            if stat_mtime is None or mtime_changed or hash_changed:
                                                is_conflict = True
                                        except (FileNotFoundError, OSError):
                                            pass

                                    if is_conflict:
                                        ts = int(time.time())
                                        conflict_name = f"{full_path.stem}.conflict.{ts}{full_path.suffix}"
                                        conflict_path = full_path.parent / conflict_name
                                        cnt = 0
                                        while conflict_path.exists():
                                            cnt += 1
                                            conflict_path = full_path.parent / f"{full_path.stem}.conflict.{ts}_{cnt}{full_path.suffix}"
                                        full_path.rename(conflict_path)

                                os.replace(tmp_file, full_path)
                            finally:
                                if tmp_file.exists():
                                    try:
                                        tmp_file.unlink()
                                    except OSError:
                                        pass

                            stat = full_path.stat()
                            sha = get_file_sha256(full_path)
                            self.db.upsert_file(path, sha, stat.st_size, stat.st_mtime, remote_rev, False)
                        except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception)) as e:
                            print(f"Warning: Failed to download {path}: {e}")
                            retryable_skips += 1
                            continue

                        if conflict_path:
                            cid = self.db.record_conflict(self.workspace_id, path, str(full_path), str(conflict_path))
                            
                            resolved_path = full_path

                            async def _on_conflict_resolved(r_path, action, target_path=resolved_path, conflict_id=cid):
                                if action in ("meld", "local"):
                                    async with self._lock:
                                        try:
                                            stat = target_path.stat()
                                            sha = get_file_sha256(target_path)
                                            new_rev = await self.api.upload_file(r_path, target_path)
                                            rev_val = new_rev.get("revision") if isinstance(new_rev, dict) else new_rev
                                            self.db.upsert_file(r_path, sha, stat.st_size, stat.st_mtime, rev_val, False)
                                            self.db.resolve_conflict_record(conflict_id, action)
                                            print(f"[{self.sync_dir.name}] Resolved conflict uploaded: {r_path} (rev {rev_val})", flush=True)
                                        except Exception as err:
                                            print(f"[{self.sync_dir.name}] Failed to upload resolved conflict {r_path}: {err}", flush=True)
                                else:
                                    self.db.resolve_conflict_record(conflict_id, action)

                            task = asyncio.create_task(handle_conflict_notification(
                                workspace=self.workspace_id,
                                rel_path=path,
                                server_file=full_path,
                                conflict_file=conflict_path,
                                on_resolve=_on_conflict_resolved,
                            ))
                            self._tasks.add(task)
                            task.add_done_callback(self._tasks.discard)
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

            if retryable_skips:
                noun = "entry" if retryable_skips == 1 else "entries"
                print(f"[{self.sync_dir.name}] Warning: {retryable_skips} {noun} will be retried on the next reconcile; server revision not advanced", flush=True)
            else:
                self.db.set_last_server_revision(latest_rev)

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
                        try:
                            await self.api.delete_file(rel_path, expected_revision=local_file['server_revision'])
                            self.db.delete_file(rel_path)
                        except httpx.HTTPStatusError as e:
                            if e.response.status_code == 409:
                                pass
                            elif e.response.status_code == 404:
                                self.db.delete_file(rel_path)
                            elif is_auth_error(e):
                                raise
                            else:
                                print(f"[{self.sync_dir.name}] Warning: deleting {rel_path} on the server failed: HTTP {e.response.status_code}", flush=True)

            # 3. Process local changes
            for root, dirs, files in os.walk(self.sync_dir):
                dirs[:] = [d for d in dirs if d != ".syncme" and not self.is_ignored(str((Path(root) / d).relative_to(self.sync_dir)).replace(os.sep, "/") + "/")]
                    
                for file in files:
                    full_path = Path(root) / file
                    rel_path = str(full_path.relative_to(self.sync_dir)).replace(os.sep, "/")
                    
                    if self.is_ignored(rel_path):
                        continue
                    
                    if full_path.is_symlink():
                        if not self.replace_symlinks:
                            continue
                        if not full_path.is_file():
                            continue
                            
                    try:
                        stat = full_path.stat()
                    except (FileNotFoundError, OSError):
                        continue
                    
                    local_file = self.db.get_file(rel_path)
                    
                    needs_upload = False
                    expected_rev = 0
                    if not local_file:
                        needs_upload = True
                    elif local_file['mtime'] < stat.st_mtime:
                        needs_upload = True
                        expected_rev = local_file['server_revision']
                        
                    if needs_upload:
                        sha = get_file_sha256(full_path)
                        if local_file and sha == local_file['sha256']:
                            self.db.upsert_file(rel_path, sha, stat.st_size, stat.st_mtime, local_file['server_revision'], False)
                            continue

                        with open(full_path, "rb") as f:
                            content = f.read()
                            
                        try:
                            resp = await self.api.upload_file(rel_path, content, expected_revision=expected_rev)
                            self.db.upsert_file(rel_path, sha, stat.st_size, stat.st_mtime, resp['revision'], False)
                            try:
                                print(f"[{self.sync_dir.name}] Uploaded {rel_path} (rev {resp['revision']})", flush=True)
                            except Exception:
                                pass
                        except httpx.HTTPStatusError as e:
                            if is_auth_error(e):
                                raise
                            if e.response.status_code != 409:      # 409 = someone else changed it: handled by the next reconcile
                                print(f"[{self.sync_dir.name}] Warning: upload of {rel_path} failed: HTTP {e.response.status_code}", flush=True)
                        except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception)) as e:
                            print(f"Warning: Failed to upload {rel_path}: {e}")

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
            local_files = {}
            for root, dirs, files in os.walk(self.sync_dir):
                dirs[:] = [d for d in dirs if d != ".syncme" and not self.is_ignored(str((Path(root) / d).relative_to(self.sync_dir)).replace(os.sep, "/") + "/")]
                for file in files:
                    full_path = Path(root) / file
                    rel_path = str(full_path.relative_to(self.sync_dir)).replace(os.sep, "/")
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
            
            import httpx
            # 3. Process each file
            all_paths = set(remote_files.keys()).union(set(local_files.keys()))
            for rel_path in all_paths:
                if self.is_ignored(rel_path):
                    continue
                    
                in_remote = rel_path in remote_files
                in_local = rel_path in local_files

                # Ask outside the per-entry guard below: an aborted prompt (click.Abort on
                # Ctrl-C is an Exception) must abort the sync, not count as one failed file.
                is_conflict = in_local and in_remote and local_files[rel_path]["sha256"] != remote_files[rel_path].get('sha256')
                choice = None
                if is_conflict:
                    summary["conflicts"] += 1
                    l_file = local_files[rel_path]
                    choice = conflict_callback(rel_path, remote_files[rel_path], {"size": l_file["stat"].st_size, "mtime": l_file["stat"].st_mtime})

                try:
                    if in_local and not in_remote:
                        # Upload
                        l_file = local_files[rel_path]
                        with open(l_file["path"], "rb") as f:
                            content = f.read()
                        try:
                            resp = await self.api.upload_file(rel_path, content, expected_revision=0)
                            self.db.upsert_file(rel_path, l_file["sha256"], l_file["stat"].st_size, l_file["stat"].st_mtime, resp['revision'], False)
                            summary["uploaded"] += 1
                        except httpx.HTTPStatusError as e:
                            if is_auth_error(e):
                                raise
                            if e.response.status_code != 409:      # 409 = someone else changed it: handled by the next reconcile
                                print(f"[{self.sync_dir.name}] Warning: upload of {rel_path} failed: HTTP {e.response.status_code}", flush=True)
                                summary["failed"] += 1
                        except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception)) as e:
                            print(f"Warning: Failed to upload {rel_path}: {e}")
                            summary["failed"] += 1
                    elif in_remote and not in_local:
                        # Download
                        r_file = remote_files[rel_path]
                        remote_rev = r_file['revision']
                        full_path = safe_join(self.sync_dir, rel_path)
                        if full_path.is_symlink() and not self.replace_symlinks:
                            continue
                        try:
                            content = await self._fetch_verified(rel_path, r_file.get('sha256'))
                            if content is None:
                                summary["failed"] += 1
                                continue
                            if full_path.is_symlink():
                                # Only reachable with replace_symlinks=True; the link is
                                # replaced only after the content has been verified.
                                full_path.unlink()
                            full_path.parent.mkdir(parents=True, exist_ok=True)
                            with open(full_path, "wb") as f:
                                f.write(content)
                            stat = full_path.stat()
                            sha = get_file_sha256(full_path)
                            self.db.upsert_file(rel_path, sha, stat.st_size, stat.st_mtime, remote_rev, False)
                            summary["downloaded"] += 1
                        except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception)) as e:
                            print(f"Warning: Failed to download {rel_path}: {e}")
                            summary["failed"] += 1
                    elif in_local and in_remote:
                        l_file = local_files[rel_path]
                        r_file = remote_files[rel_path]

                        if not is_conflict:
                            self.db.upsert_file(rel_path, l_file["sha256"], l_file["stat"].st_size, l_file["stat"].st_mtime, r_file['revision'], False)
                            summary["unchanged"] += 1
                        else:
                            # Conflict: in the "server" and "both" branches nothing local may
                            # change (archive, unlink, rename) before the content is verified.
                            if choice in ("s", "server"):
                                remote_rev = r_file['revision']
                                full_path = safe_join(self.sync_dir, rel_path)
                                try:
                                    content = await self._fetch_verified(rel_path, r_file.get('sha256'))
                                    if content is None:
                                        summary["failed"] += 1
                                        continue
                                    archive_file(self.sync_dir, rel_path)
                                    if full_path.is_symlink():
                                        full_path.unlink()
                                    full_path.parent.mkdir(parents=True, exist_ok=True)
                                    with open(full_path, "wb") as f:
                                        f.write(content)
                                    stat = full_path.stat()
                                    sha = get_file_sha256(full_path)
                                    self.db.upsert_file(rel_path, sha, stat.st_size, stat.st_mtime, remote_rev, False)
                                except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception)) as e:
                                    print(f"Warning: Failed to download {rel_path} during conflict resolution: {e}")
                                    summary["failed"] += 1
                            elif choice in ("l", "local"):
                                with open(l_file["path"], "rb") as f:
                                    content = f.read()
                                try:
                                    resp = await self.api.upload_file(rel_path, content, expected_revision=r_file['revision'])
                                    self.db.upsert_file(rel_path, l_file["sha256"], l_file["stat"].st_size, l_file["stat"].st_mtime, resp['revision'], False)
                                except httpx.HTTPStatusError as e:
                                    if is_auth_error(e):
                                        raise
                                    if e.response.status_code != 409:      # 409 = someone else changed it: handled by the next reconcile
                                        print(f"[{self.sync_dir.name}] Warning: upload of {rel_path} failed: HTTP {e.response.status_code}", flush=True)
                                        summary["failed"] += 1
                                except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception)) as e:
                                    print(f"Warning: Failed to upload {rel_path} during conflict resolution: {e}")
                                    summary["failed"] += 1
                            elif choice in ("b", "both"):
                                remote_rev = r_file['revision']
                                full_path = safe_join(self.sync_dir, rel_path)
                                try:
                                    content = await self._fetch_verified(rel_path, r_file.get('sha256'))
                                    if content is None:
                                        summary["failed"] += 1
                                        continue
                                    conflict_name = f"{full_path.stem}.conflict.{int(time.time())}{full_path.suffix}"
                                    conflict_path = full_path.parent / conflict_name
                                    full_path.rename(conflict_path)
                                    full_path.parent.mkdir(parents=True, exist_ok=True)
                                    with open(full_path, "wb") as f:
                                        f.write(content)
                                    stat = full_path.stat()
                                    sha = get_file_sha256(full_path)
                                    self.db.upsert_file(rel_path, sha, stat.st_size, stat.st_mtime, remote_rev, False)
                                    self.db.record_conflict(self.workspace_id, rel_path, str(full_path), str(conflict_path))
                                except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception)) as e:
                                    print(f"Warning: Failed to download {rel_path} during conflict resolution: {e}")
                                    summary["failed"] += 1
                except Exception as e:
                    if is_auth_error(e):
                        raise
                    # One poisoned entry must not abort the whole sync. Exception rather than
                    # BaseException, so cancellation still propagates.
                    print(f"[{self.sync_dir.name}] Warning: skipping {rel_path!r}: {e!r}", flush=True)
                    summary["failed"] += 1
            
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

    async def _sse_loop(self):
        label = self.sync_dir.name
        delay = self.sse_initial_delay
        while True:
            opened_at = None
            failure = None
            try:
                async for _event in self.api.listen_events():
                    # The first item is the synthetic "connected" event: the stream is open.
                    # Reconcile on it (and on every real event), so nothing that happened
                    # while we were disconnected is lost.
                    if opened_at is None:
                        opened_at = self.sse_clock()
                    await self._reconcile_safely()
                # the server closed the stream: fall through and reconnect
            except Exception as e:
                if is_auth_error(e):
                    raise
                failure = e
            # Only a stream that stayed open long enough is a healthy connection that resets the
            # backoff. Resetting on every event made a server that closes the stream after each
            # event a one-second reconnect loop.
            if opened_at is not None and self.sse_clock() - opened_at >= self.sse_healthy_seconds:
                delay = self.sse_initial_delay
            if failure is not None:
                print(f"[{label}] Event stream interrupted ({describe_stream_error(failure)}); reconnecting in {delay:.0f}s", flush=True)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.sse_max_delay)

    async def _reconcile_safely(self):
        label = self.sync_dir.name
        try:
            await self.reconcile()
        except Exception as e:
            if is_auth_error(e):
                raise
            print(f"[{label}] Reconcile failed: {type(e).__name__}: {e}", flush=True)

    async def _poll_loop(self):
        if not self.poll_interval_seconds or self.poll_interval_seconds <= 0:
            return
        while True:
            await asyncio.sleep(self.poll_interval_seconds)
            print(f"[{self.sync_dir.name}] Periodic fallback poll triggering reconcile...", flush=True)
            await self._reconcile_safely()

    async def run(self):
        await self.reconcile()
        await asyncio.gather(
            self._watch_loop(),
            self._sse_loop(),
            self._poll_loop()
        )
