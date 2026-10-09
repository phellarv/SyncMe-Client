import asyncio
import os
import time
from pathlib import Path
import httpx

from client.db import LocalDB
from client.api import APIClient
from client.engine import get_file_sha256
from client.config import ServerConfig
from client.errors import describe_stream_error, is_auth_error
from client.archive import archive_file
from client.watcher import Watcher
from client.notifications import handle_conflict_notification
from client.pathsafe import sha256_matches

class FileGroupEngine:
    def __init__(self, workspace_id, files, db, api, server_config):
        self.workspace_id = workspace_id
        self.files = files
        self.db = db
        self.api = api
        self.server_config = server_config
        self._lock = asyncio.Lock()
        self._tasks: set = set()
        self.sse_initial_delay = 1.0   # event stream reconnect backoff (seconds)
        self.sse_max_delay = 60.0
        self.sse_healthy_seconds = 30.0   # a stream open at least this long resets the backoff
        self.sse_clock = time.monotonic   # injectable for tests

    def _get_f_conf(self, path):
        for f in self.files:
            if f.remote_path == path:
                return f
        return None

    async def _fetch_verified(self, path, expected_sha256):
        """Download `path` and check it against the server's sha256; None means a mismatch (nothing may be written)."""
        content = await self.api.download_file(path)
        if not sha256_matches(content, expected_sha256):
            print(f"[{self.workspace_id}] Warning: hash mismatch for {path}, not written", flush=True)
            return None
        return content

    async def reconcile(self):
        if not self.db.is_initialized():
            raise RuntimeError(f"SyncMe is not initialized. Please run 'syncme-client init' first. ({self.workspace_id})")
            
        async with self._lock:
            remote_state = await self.api.get_state(self.db.get_last_server_revision())
            latest_rev = remote_state.get('latest_revision', remote_state.get('last_revision', 0))

            # An entry whose fetch failed (network or HTTP error, hash mismatch, local I/O error)
            # is counted and the stored revision is then left alone, so the next reconcile
            # fetches it again. Deliberate skips are not counted.
            retryable_skips = 0
            for file_info in remote_state.get('files', []):
                path = file_info['path']
                f_conf = self._get_f_conf(path)
                if not f_conf: continue
                
                remote_rev = file_info['revision']
                is_deleted = file_info.get('is_deleted', False)
                local_file = self.db.get_file(path)
                full_path = f_conf.local_path
                
                if is_deleted:
                    # A deletion is replayed whenever a retryable skip keeps the stored revision
                    # back, so it must never destroy anything the user has made since: only a
                    # tracked file that is not newer than the deletion is archived and removed.
                    if not full_path.exists():
                        self.db.delete_file(path)
                    elif local_file and local_file['server_revision'] >= remote_rev:
                        # Stale: the local copy is at least as new as this deletion.
                        continue
                    elif local_file is None:
                        print(f"[{self.workspace_id}] Warning: not deleting {path!r}: the server deleted it, but the local file is not tracked here (kept as is)", flush=True)
                    else:
                        archive_file(full_path.parent, full_path.name)
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
                    # initial_sync that failed and so left the stored revision unchanged) is user
                    # data: never overwrite it silently. A symlink is not, so it keeps following
                    # replace_symlinks below.
                    untracked_file = local_file is None and full_path.is_file() and not full_path.is_symlink()
                    if untracked_file:
                        local_sha = get_file_sha256(full_path)
                        if file_info.get('sha256') and local_sha == file_info['sha256']:
                            # Already identical to the server version: adopt it, no download.
                            stat = full_path.stat()
                            self.db.upsert_file(path, local_sha, stat.st_size, stat.st_mtime, remote_rev, False)
                            continue

                    # Decide whether this is a conflict, but do not touch the local file yet:
                    # the server content must be fetched and verified first.
                    is_conflict = untracked_file  # differing, or unprovable, content
                    stat_mtime = None
                    if full_path.is_file() and not full_path.is_symlink():
                        try:
                            stat_mtime = full_path.stat().st_mtime
                        except (FileNotFoundError, OSError):
                            stat_mtime = None

                    if local_file and local_file['server_revision'] < remote_rev and full_path.exists():
                        mtime_check = stat_mtime if stat_mtime is not None else local_file['mtime']
                        is_conflict = local_file['mtime'] < mtime_check

                    if not is_conflict and full_path.is_symlink() and not f_conf.replace_symlinks:
                        continue

                    conflict_path = None
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
                    except Exception as e:
                        if is_auth_error(e):
                            raise
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
                                        print(f"[{self.workspace_id}] Resolved conflict uploaded: {r_path} (rev {rev_val})", flush=True)
                                    except Exception as err:
                                        print(f"[{self.workspace_id}] Failed to upload resolved conflict {r_path}: {err}", flush=True)
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

            if retryable_skips:
                noun = "entry" if retryable_skips == 1 else "entries"
                print(f"[{self.workspace_id}] Warning: {retryable_skips} {noun} will be retried on the next reconcile; server revision not advanced", flush=True)
            else:
                self.db.set_last_server_revision(latest_rev)

            for f_conf in self.files:
                path = f_conf.remote_path
                local_file = self.db.get_file(path)
                full_path = f_conf.local_path
                if local_file and not full_path.exists():
                    try:
                        await self.api.delete_file(path, expected_revision=local_file['server_revision'])
                        self.db.delete_file(path)
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 404:
                            self.db.delete_file(path)
                        elif is_auth_error(e):
                            raise
                        elif e.response.status_code != 409:
                            print(f"[{self.workspace_id}] Warning: deleting {path} on the server failed: HTTP {e.response.status_code}", flush=True)

            for f_conf in self.files:
                path = f_conf.remote_path
                full_path = f_conf.local_path
                
                if full_path.is_symlink():
                    if not f_conf.replace_symlinks: continue
                    if not full_path.is_file(): continue
                        
                try:
                    stat = full_path.stat()
                except (FileNotFoundError, OSError):
                    continue
                    
                local_file = self.db.get_file(path)
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
                        self.db.upsert_file(path, sha, stat.st_size, stat.st_mtime, local_file['server_revision'], False)
                        continue

                    with open(full_path, "rb") as f:
                        content = f.read()
                    try:
                        resp = await self.api.upload_file(path, content, expected_revision=expected_rev)
                        self.db.upsert_file(path, sha, stat.st_size, stat.st_mtime, resp['revision'], False)
                    except httpx.HTTPStatusError as e:
                        if is_auth_error(e):
                            raise
                        if e.response.status_code != 409:      # 409 = someone else changed it: handled by the next reconcile
                            print(f"[{self.workspace_id}] Warning: upload of {path} failed: HTTP {e.response.status_code}", flush=True)
                    except Exception as e:
                        print(f"Warning: Failed to upload {path}: {e}")

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
                    if not f_conf.replace_symlinks: continue
                    if not full_path.is_file(): continue
                try:
                    stat = full_path.stat()
                except (FileNotFoundError, OSError):
                    continue
                local_files[path] = {
                    "path": full_path,
                    "stat": stat,
                    "sha256": get_file_sha256(full_path),
                    "f_conf": f_conf
                }

            summary = {"uploaded": 0, "downloaded": 0, "conflicts": 0, "unchanged": 0, "failed": 0}
            all_paths = set(remote_files.keys()).union(set(local_files.keys()))
            
            for path in all_paths:
                f_conf = self._get_f_conf(path)
                if not f_conf: continue
                full_path = f_conf.local_path
                
                in_remote = path in remote_files
                in_local = path in local_files
                
                if in_local and not in_remote:
                    l_file = local_files[path]
                    with open(l_file["path"], "rb") as f: content = f.read()
                    try:
                        resp = await self.api.upload_file(path, content, expected_revision=0)
                        self.db.upsert_file(path, l_file["sha256"], l_file["stat"].st_size, l_file["stat"].st_mtime, resp['revision'], False)
                        summary["uploaded"] += 1
                    except Exception as e:
                        if is_auth_error(e):
                            raise
                        summary["failed"] += 1
                elif in_remote and not in_local:
                    r_file = remote_files[path]
                    if full_path.is_symlink() and not f_conf.replace_symlinks: continue
                    try:
                        content = await self._fetch_verified(path, r_file.get('sha256'))
                        if content is None:
                            summary["failed"] += 1
                            continue
                        if full_path.is_symlink():
                            # Only reachable with replace_symlinks=True; the link is replaced
                            # only after the content has been verified.
                            full_path.unlink()
                        full_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(full_path, "wb") as f: f.write(content)
                        stat = full_path.stat()
                        sha = get_file_sha256(full_path)
                        self.db.upsert_file(path, sha, stat.st_size, stat.st_mtime, r_file['revision'], False)
                        summary["downloaded"] += 1
                    except Exception as e:
                        if is_auth_error(e):
                            raise
                        summary["failed"] += 1
                elif in_local and in_remote:
                    l_file = local_files[path]
                    r_file = remote_files[path]
                    if l_file["sha256"] == r_file["sha256"]:
                        self.db.upsert_file(path, l_file["sha256"], l_file["stat"].st_size, l_file["stat"].st_mtime, r_file['revision'], False)
                        summary["unchanged"] += 1
                    else:
                        summary["conflicts"] += 1
                        choice = conflict_callback(path, r_file, {"size": l_file["stat"].st_size, "mtime": l_file["stat"].st_mtime})
                        # In the "server" and "both" branches nothing local may change (archive,
                        # unlink, rename) before the content is fetched and verified.
                        if choice in ("s", "server"):
                            try:
                                content = await self._fetch_verified(path, r_file.get('sha256'))
                                if content is None:
                                    summary["failed"] += 1
                                    continue
                                archive_file(full_path.parent, full_path.name)
                                if full_path.is_symlink(): full_path.unlink()
                                full_path.parent.mkdir(parents=True, exist_ok=True)
                                with open(full_path, "wb") as f: f.write(content)
                                stat = full_path.stat()
                                sha = get_file_sha256(full_path)
                                self.db.upsert_file(path, sha, stat.st_size, stat.st_mtime, r_file['revision'], False)
                            except Exception as e:
                                if is_auth_error(e):
                                    raise
                                summary["failed"] += 1
                        elif choice in ("l", "local"):
                            with open(l_file["path"], "rb") as f: content = f.read()
                            try:
                                resp = await self.api.upload_file(path, content, expected_revision=r_file['revision'])
                                self.db.upsert_file(path, l_file["sha256"], l_file["stat"].st_size, l_file["stat"].st_mtime, resp['revision'], False)
                            except Exception as e:
                                if is_auth_error(e):
                                    raise
                                summary["failed"] += 1
                        elif choice in ("b", "both"):
                            try:
                                content = await self._fetch_verified(path, r_file.get('sha256'))
                                if content is None:
                                    summary["failed"] += 1
                                    continue
                                conflict_name = f"{full_path.stem}.conflict.{int(time.time())}{full_path.suffix}"
                                conflict_path = full_path.parent / conflict_name
                                full_path.rename(conflict_path)
                                full_path.parent.mkdir(parents=True, exist_ok=True)
                                with open(full_path, "wb") as f: f.write(content)
                                stat = full_path.stat()
                                sha = get_file_sha256(full_path)
                                self.db.upsert_file(path, sha, stat.st_size, stat.st_mtime, r_file['revision'], False)
                                self.db.record_conflict(self.workspace_id, path, str(full_path), str(conflict_path))
                            except Exception as e:
                                if is_auth_error(e):
                                    raise
                                summary["failed"] += 1

            self.db.set_initialized(True)
            # Only a clean run may move the stored revision forward. With failures it stays
            # where it was, so the next reconcile fetches the same entries again.
            if summary["failed"] == 0:
                self.db.set_last_server_revision(latest_rev)
            return summary

    async def _sse_loop(self):
        label = self.workspace_id
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
        label = self.workspace_id
        try:
            await self.reconcile()
        except Exception as e:
            if is_auth_error(e):
                raise
            print(f"[{label}] Reconcile failed: {type(e).__name__}: {e}", flush=True)

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
                w = Watcher(parent, self.server_config.debounce_seconds, replace_symlinks, recursive=False)
                w.start()
                watchers.append(w)
            
            async def pull_events(w):
                async for batch in w.get_batches():
                    should_reconcile = False
                    for rel_path, _ in batch:
                        for f in self.files:
                            if f.local_path.name == rel_path or f.local_path.name == Path(rel_path).name:
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

    async def _poll_loop(self):
        interval = getattr(self.server_config, 'poll_interval_seconds', 300)
        if not interval or interval <= 0:
            return
        while True:
            await asyncio.sleep(interval)
            print(f"[{self.workspace_id}] Periodic fallback poll triggering reconcile...", flush=True)
            await self._reconcile_safely()

    async def run(self):
        await self.reconcile()
        await asyncio.gather(
            self._watch_loop(),
            self._sse_loop(),
            self._poll_loop()
        )
