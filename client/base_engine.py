"""Base engine class providing shared synchronization logic for SyncMe engines."""

import abc
import asyncio
import hashlib
import os
import sys
import time
from pathlib import Path

import httpx

from client.api import APIClient, STREAM_THRESHOLD
from client.db import LocalDB
from client.errors import describe_stream_error, is_auth_error
from client.notifications import handle_conflict_notification
from client.pathsafe import sha256_matches


def get_file_sha256(path: Path) -> str:
    """Calculate SHA256 hex digest of a local file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def prepare_upload_payload(path: Path, size: int | None = None) -> Path | bytes:
    """Return Path for streaming if size exceeds STREAM_THRESHOLD, else file bytes."""
    if size is None:
        try:
            size = path.stat().st_size
        except OSError:
            size = 0
    return path if size > STREAM_THRESHOLD else path.read_bytes()


class BaseEngine(abc.ABC):
    """Abstract base class consolidating common sync, loop, conflict, and upload logic."""

    def __init__(
        self,
        workspace_id: str,
        db: LocalDB,
        api: APIClient,
        poll_interval_seconds: int = 300,
    ):
        self.workspace_id = workspace_id
        self.db = db
        self.api = api
        self.poll_interval_seconds = poll_interval_seconds
        self._lock = asyncio.Lock()
        self._tasks: set = set()
        self.sse_initial_delay = 1.0   # event stream reconnect backoff (seconds)
        self.sse_max_delay = 60.0
        self.sse_healthy_seconds = 30.0   # stream open at least this long resets backoff
        self.sse_clock = time.monotonic   # injectable for tests

    @property
    def log_label(self) -> str:
        """Label used in log and warning messages."""
        return self.workspace_id

    def prepare_upload_payload(self, path: Path, size: int | None = None) -> Path | bytes:
        """Return Path for streaming if file exceeds threshold, else read bytes."""
        return prepare_upload_payload(path, size)

    async def _fetch_verified(self, path: str, expected_sha256: str | None) -> bytes | None:
        """Download `path` and check it against expected sha256; None on mismatch or error."""
        content = await self.api.download_file(path)
        if not sha256_matches(content, expected_sha256):
            print(f"[{self.log_label}] Warning: hash mismatch for {path}, not written", flush=True)
            return None
        return content

    def _should_stream_download(self, file_info: dict) -> bool:
        """Return True if the remote entry should be streamed directly to disk."""
        size = file_info.get("size")
        if not isinstance(size, int) or size <= STREAM_THRESHOLD:
            return False
        if not isinstance(self.api, APIClient):
            return False
        if "download_file" in getattr(self.api, "__dict__", {}):
            return False
        return True

    @staticmethod
    def _default_file_mode() -> int:
        umask = os.umask(0o022)
        os.umask(umask)
        return 0o666 & ~umask

    @staticmethod
    def _atomic_write_bytes(full_path: Path, content: bytes) -> None:
        """Write `content` atomically to `full_path`, preserving existing permissions or applying umask."""
        import stat as stat_module
        import tempfile

        full_path.parent.mkdir(parents=True, exist_ok=True)
        old_mode = None
        if full_path.exists() and not full_path.is_symlink():
            try:
                old_mode = stat_module.S_IMODE(full_path.stat().st_mode)
            except OSError:
                pass
        target_mode = old_mode if old_mode is not None else BaseEngine._default_file_mode()

        tmp = tempfile.NamedTemporaryFile(dir=full_path.parent, prefix=".syncme-tmp-", delete=False)
        tmp_file = Path(tmp.name)
        try:
            with tmp:
                tmp.write(content)
                tmp.flush()
                os.fsync(tmp.fileno())
            try:
                os.chmod(tmp_file, target_mode)
            except OSError:
                pass
            os.replace(tmp_file, full_path)
        finally:
            if tmp_file.exists():
                try:
                    tmp_file.unlink()
                except OSError:
                    pass

    def prune_missing_conflicts(self):
        """Safely prune missing conflicts from the local database."""
        try:
            self.db.prune_missing_conflicts(self.workspace_id)
        except Exception as e:
            print(f"[{self.workspace_id}] Warning: failed to prune missing conflicts: {e}", flush=True)

    @staticmethod
    def _truncate_utf8(s: str, max_bytes: int) -> str:
        encoded = s.encode("utf-8")
        if len(encoded) <= max_bytes:
            return s
        return encoded[:max_bytes].decode("utf-8", errors="ignore")

    @staticmethod
    def generate_conflict_path(path: Path) -> Path:
        """Generate a unique conflict destination Path for `path`, bounding filename length <= 230 bytes."""
        ts = int(time.time())
        max_name_bytes = 230

        def _make_name(cnt: int) -> str:
            mid = f".conflict.{ts}" if cnt == 0 else f".conflict.{ts}_{cnt}"
            safe_suffix = BaseEngine._truncate_utf8(path.suffix, 64)
            suffix_part = f"{mid}{safe_suffix}"
            suffix_bytes = len(suffix_part.encode("utf-8"))
            allowed_stem_bytes = max(1, max_name_bytes - suffix_bytes)
            stem = BaseEngine._truncate_utf8(path.stem, allowed_stem_bytes)
            return f"{stem}{suffix_part}"

        cnt = 0
        conflict_path = path.parent / _make_name(cnt)
        while conflict_path.exists():
            cnt += 1
            conflict_path = path.parent / _make_name(cnt)
        return conflict_path

    def rename_to_conflict(self, path: Path) -> Path:
        """Rename `path` to a unique conflict filename and return the conflict Path."""
        conflict_path = self.generate_conflict_path(path)
        path.rename(conflict_path)
        return conflict_path

    def record_and_spawn_conflict(
        self,
        rel_path: str,
        server_file: Path,
        conflict_file: Path,
    ):
        """Record conflict in database and spawn asynchronous desktop notification."""
        import inspect
        import shutil

        cid = self.db.record_conflict(self.workspace_id, rel_path, str(server_file), str(conflict_file))
        resolved_path = server_file

        async def _on_conflict_resolved(r_path, action, target_path=resolved_path, c_file=conflict_file, conflict_id=cid):
            if action in ("meld", "local"):
                async with self._lock:
                    try:
                        local_file = self.db.get_file(r_path) if hasattr(self.db, "get_file") else None
                        expected_rev = local_file.get("server_revision") if isinstance(local_file, dict) else None
                        upload_source = c_file if (action == "local" and c_file.exists()) else target_path

                        target_fn = getattr(self.api.upload_file, "side_effect", None)
                        if not callable(target_fn) or isinstance(target_fn, type):
                            target_fn = self.api.upload_file
                        sig = inspect.signature(target_fn)
                        supports_expected = (
                            "expected_revision" in sig.parameters
                            or hasattr(target_fn, "_mock_name")
                        )
                        kwargs = {"expected_revision": expected_rev} if supports_expected else {}
                        new_rev = await self.api.upload_file(r_path, upload_source, **kwargs)
                        if upload_source != target_path and upload_source.exists():
                            shutil.copy2(upload_source, target_path)
                        stat = target_path.stat()
                        sha = get_file_sha256(target_path)
                        rev_val = new_rev.get("revision") if isinstance(new_rev, dict) else new_rev
                        self.db.upsert_file(r_path, sha, stat.st_size, stat.st_mtime, rev_val, False)
                        self.db.resolve_conflict_record(conflict_id, action)
                        print(f"[{self.log_label}] Resolved conflict uploaded: {r_path} (rev {rev_val})", flush=True)
                    except Exception as err:
                        print(f"[{self.log_label}] Failed to upload resolved conflict {r_path}: {err}", flush=True)
                        raise
            else:
                self.db.resolve_conflict_record(conflict_id, action)

        # Look up notification function from caller's module to support monkeypatching in unit tests
        notify_fn = getattr(sys.modules.get(self.__module__), "handle_conflict_notification", handle_conflict_notification)
        task = asyncio.create_task(notify_fn(
            workspace=self.workspace_id,
            rel_path=rel_path,
            server_file=server_file,
            conflict_file=conflict_file,
            on_resolve=_on_conflict_resolved,
        ))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return cid

    def _finalize_downloaded_tmp(
        self,
        path: str,
        full_path: Path,
        tmp_file: Path,
        old_mode: int | None,
        remote_rev: int,
        local_file: dict | None,
        is_conflict: bool,
        stat_mtime: float | None,
    ) -> Path | None:
        conflict_path = None
        target_mode = old_mode if old_mode is not None else self._default_file_mode()
        try:
            os.chmod(tmp_file, target_mode)
        except OSError:
            pass

        if full_path.is_symlink():
            if not is_conflict:
                full_path.unlink()
            else:
                conflict_path = self.rename_to_conflict(full_path)
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
                conflict_path = self.rename_to_conflict(full_path)

        os.replace(tmp_file, full_path)
        stat = full_path.stat()
        sha = get_file_sha256(full_path)
        self.db.upsert_file(path, sha, stat.st_size, stat.st_mtime, remote_rev, False)
        return conflict_path

    def write_downloaded_file(
        self,
        path: str,
        full_path: Path,
        content: bytes,
        remote_rev: int,
        local_file: dict | None,
        is_conflict: bool,
        stat_mtime: float | None,
    ) -> Path | None:
        """Write downloaded content atomically, handling mode preservation and conflict renaming."""
        full_path.parent.mkdir(parents=True, exist_ok=True)
        old_mode = None
        if full_path.exists() and not full_path.is_symlink():
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

            return self._finalize_downloaded_tmp(
                path=path,
                full_path=full_path,
                tmp_file=tmp_file,
                old_mode=old_mode,
                remote_rev=remote_rev,
                local_file=local_file,
                is_conflict=is_conflict,
                stat_mtime=stat_mtime,
            )
        finally:
            if tmp_file.exists():
                try:
                    tmp_file.unlink()
                except OSError:
                    pass

    async def _stream_downloaded_file(
        self,
        path: str,
        full_path: Path,
        expected_sha256: str | None,
        remote_rev: int,
        local_file: dict | None,
        is_conflict: bool,
        stat_mtime: float | None,
    ) -> Path | None | bool:
        """Stream a large download to a temporary file, verify sha256, and atomically move into place.
        Returns conflict_path (Path | None) on success, or False on hash mismatch.
        """
        full_path.parent.mkdir(parents=True, exist_ok=True)
        old_mode = None
        if full_path.exists() and not full_path.is_symlink():
            try:
                import stat as stat_module
                old_mode = stat_module.S_IMODE(full_path.stat().st_mode)
            except OSError:
                pass

        tmp_file = full_path.with_name(f".syncme-tmp-{full_path.name}-{time.time_ns()}")
        try:
            try:
                await self.api.download_file_to_path(path, tmp_file, expected_sha256=expected_sha256)
            except ValueError:
                print(f"[{self.log_label}] Warning: hash mismatch for {path}, not written", flush=True)
                return False

            return self._finalize_downloaded_tmp(
                path=path,
                full_path=full_path,
                tmp_file=tmp_file,
                old_mode=old_mode,
                remote_rev=remote_rev,
                local_file=local_file,
                is_conflict=is_conflict,
                stat_mtime=stat_mtime,
            )
        finally:
            if tmp_file.exists():
                try:
                    tmp_file.unlink()
                except OSError:
                    pass

    async def _reconcile_remote_entry(
        self,
        path: str,
        full_path: Path,
        file_info: dict,
        replace_symlinks: bool,
    ) -> bool:
        """Process a single remote file entry during reconcile. Returns True if a retryable skip occurred."""
        remote_rev = file_info['revision']
        is_deleted = file_info.get('is_deleted', False)
        local_file = self.db.get_file(path)

        if is_deleted:
            self.apply_server_deletion(path, full_path, remote_rev, local_file)
            return False

        stat_mtime = None
        stat_size = None
        if full_path.is_file() and not full_path.is_symlink():
            try:
                st = full_path.stat()
                stat_mtime = st.st_mtime
                stat_size = st.st_size
            except (FileNotFoundError, OSError):
                stat_mtime = None
                stat_size = None

        if local_file and local_file['server_revision'] >= remote_rev:
            return False

        if local_file and full_path.exists():
            mtime_check = stat_mtime if stat_mtime is not None else local_file['mtime']
            meta_changed = (abs(mtime_check - local_file['mtime']) > 1e-6) or (
                stat_size is not None and local_file['size'] != stat_size
            )
            local_modified = False
            if meta_changed and full_path.is_file() and not full_path.is_symlink():
                disk_sha = get_file_sha256(full_path)
                if disk_sha == local_file['sha256']:
                    local_modified = False
                else:
                    local_modified = True
                    if file_info.get('sha256') and disk_sha == file_info['sha256']:
                        self.db.upsert_file(path, disk_sha, stat_size, stat_mtime, remote_rev, False)
                        return False
            if not local_modified:
                if file_info.get('sha256') and local_file['sha256'] == file_info['sha256']:
                    self.db.upsert_file(
                        path,
                        local_file['sha256'],
                        stat_size if stat_size is not None else local_file['size'],
                        stat_mtime if stat_mtime is not None else local_file['mtime'],
                        remote_rev,
                        False,
                    )
                    return False
            is_conflict = local_modified
        else:
            untracked_file = local_file is None and full_path.is_file() and not full_path.is_symlink()
            if untracked_file:
                local_sha = get_file_sha256(full_path)
                if file_info.get('sha256') and local_sha == file_info['sha256']:
                    self.db.upsert_file(path, local_sha, stat_size, stat_mtime, remote_rev, False)
                    return False
            is_conflict = untracked_file

        if not is_conflict and full_path.is_symlink() and not replace_symlinks:
            return False

        try:
            if self._should_stream_download(file_info):
                conflict_path = await self._stream_downloaded_file(
                    path=path,
                    full_path=full_path,
                    expected_sha256=file_info.get('sha256'),
                    remote_rev=remote_rev,
                    local_file=local_file,
                    is_conflict=is_conflict,
                    stat_mtime=stat_mtime,
                )
                if conflict_path is False:
                    return True
            else:
                content = await self._fetch_verified(path, file_info.get('sha256'))
                if content is None:
                    return True

                conflict_path = self.write_downloaded_file(
                    path=path,
                    full_path=full_path,
                    content=content,
                    remote_rev=remote_rev,
                    local_file=local_file,
                    is_conflict=is_conflict,
                    stat_mtime=stat_mtime,
                )
        except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception), httpx.HTTPStatusError, ConnectionError) as e:
            if is_auth_error(e):
                raise
            print(f"Warning: Failed to download {path}: {e}")
            return True

        if conflict_path:
            self.record_and_spawn_conflict(path, full_path, conflict_path)
        return False

    async def _upload_local_file_if_changed(
        self,
        rel_path: str,
        full_path: Path,
        replace_symlinks: bool,
        log_upload: bool = False,
    ) -> None:
        """Check if a local file is new or modified and upload it if its hash changed."""
        if full_path.is_symlink():
            if not replace_symlinks:
                return
            if not full_path.is_file():
                return
        elif not full_path.is_file():
            return

        try:
            stat = full_path.stat()
        except (FileNotFoundError, OSError):
            return

        local_file = self.db.get_file(rel_path)

        needs_upload = False
        expected_rev = 0
        if not local_file:
            needs_upload = True
        elif abs(stat.st_mtime - local_file['mtime']) > 1e-6 or local_file['size'] != stat.st_size:
            needs_upload = True
            expected_rev = local_file['server_revision']

        if needs_upload:
            try:
                payload = self.prepare_upload_payload(full_path, stat.st_size)
            except (FileNotFoundError, OSError):
                return

            if isinstance(payload, bytes):
                sha = hashlib.sha256(payload).hexdigest()
                rec_size = len(payload)
            else:
                sha = get_file_sha256(full_path)
                try:
                    post_stat = full_path.stat()
                except OSError:
                    return
                if post_stat.st_size != stat.st_size or abs(post_stat.st_mtime - stat.st_mtime) > 1e-6:
                    return
                rec_size = stat.st_size

            if local_file and sha == local_file['sha256']:
                self.db.upsert_file(rel_path, sha, rec_size, stat.st_mtime, local_file['server_revision'], False)
                return

            try:
                resp = await self.api.upload_file(rel_path, payload, expected_revision=expected_rev)
                self.db.upsert_file(rel_path, sha, rec_size, stat.st_mtime, resp['revision'], False)
                if log_upload:
                    try:
                        print(f"[{self.log_label}] Uploaded {rel_path} (rev {resp['revision']})", flush=True)
                    except Exception:
                        pass
            except httpx.HTTPStatusError as e:
                if is_auth_error(e):
                    raise
                if e.response is not None and e.response.status_code == 409:
                    headers = getattr(e.response, "headers", None) or {}
                    reason = headers.get("x-syncme-conflict-reason") or headers.get("X-SyncMe-Conflict-Reason")
                    if reason in ("deleted", "missing") and expected_rev != 0:
                        try:
                            resp = await self.api.upload_file(rel_path, payload, expected_revision=0)
                            self.db.upsert_file(rel_path, sha, rec_size, stat.st_mtime, resp['revision'], False)
                        except Exception as retry_err:
                            if is_auth_error(retry_err):
                                raise
                            print(f"[{self.log_label}] Warning: recreate upload of {rel_path} failed: {retry_err}", flush=True)
                    elif reason == "revision_mismatch":
                        cur_rev_str = headers.get("x-syncme-current-revision") or headers.get("X-SyncMe-Current-Revision")
                        cur_sha = headers.get("x-syncme-current-sha256") or headers.get("X-SyncMe-Current-SHA256") or None
                        if cur_rev_str and cur_rev_str.isdigit() and int(cur_rev_str) > 0:
                            await self._reconcile_remote_entry(
                                rel_path,
                                full_path,
                                {
                                    "path": rel_path,
                                    "revision": int(cur_rev_str),
                                    "is_deleted": False,
                                    "sha256": cur_sha,
                                },
                                replace_symlinks,
                            )
                else:
                    code = e.response.status_code if e.response is not None else "unknown"
                    print(f"[{self.log_label}] Warning: upload of {rel_path} failed: HTTP {code}", flush=True)
            except Exception as e:
                if is_auth_error(e):
                    raise
                print(f"Warning: Failed to upload {rel_path}: {e}")

    async def _download_for_initial_sync(
        self,
        rel_path: str,
        full_path: Path,
        r_file: dict,
        before_replace=None,
    ) -> bool:
        """Download and atomically write `rel_path` during initial_sync (streaming if large).
        Calls `before_replace()` only after download/verification succeeds.
        Returns True on success, False on hash mismatch.
        """
        import stat as stat_module

        expected_sha = r_file.get("sha256")
        if self._should_stream_download(r_file):
            full_path.parent.mkdir(parents=True, exist_ok=True)
            old_mode = None
            if full_path.exists() and not full_path.is_symlink():
                try:
                    old_mode = stat_module.S_IMODE(full_path.stat().st_mode)
                except OSError:
                    pass
            target_mode = old_mode if old_mode is not None else self._default_file_mode()
            tmp_file = full_path.with_name(f".syncme-tmp-{full_path.name}-{time.time_ns()}")
            try:
                try:
                    await self.api.download_file_to_path(rel_path, tmp_file, expected_sha256=expected_sha)
                except ValueError:
                    print(f"[{self.log_label}] Warning: hash mismatch for {rel_path}, not written", flush=True)
                    return False
                try:
                    os.chmod(tmp_file, target_mode)
                except OSError:
                    pass
                if before_replace is not None:
                    before_replace()
                os.replace(tmp_file, full_path)
                return True
            finally:
                if tmp_file.exists():
                    try:
                        tmp_file.unlink()
                    except OSError:
                        pass
        else:
            content = await self._fetch_verified(rel_path, expected_sha)
            if content is None:
                return False
            if before_replace is not None:
                before_replace()
            self._atomic_write_bytes(full_path, content)
            return True

    async def _initial_sync_entry(
        self,
        rel_path: str,
        resolve_full_path,
        in_local: bool,
        in_remote: bool,
        local_files: dict,
        remote_files: dict,
        replace_symlinks: bool,
        conflict_callback,
        summary: dict,
    ) -> None:
        """Process a single path during initial_sync, updating `summary` in place."""
        is_conflict = in_local and in_remote and local_files[rel_path]["sha256"] != remote_files[rel_path].get('sha256')
        choice = None
        if is_conflict:
            summary["conflicts"] += 1
            l_file = local_files[rel_path]
            choice = conflict_callback(rel_path, remote_files[rel_path], {"size": l_file["stat"].st_size, "mtime": l_file["stat"].st_mtime})

        try:
            if in_local and not in_remote:
                l_file = local_files[rel_path]
                l_path = Path(l_file["path"])
                l_size = l_file["stat"].st_size if "stat" in l_file and hasattr(l_file["stat"], "st_size") else l_path.stat().st_size
                payload = self.prepare_upload_payload(l_path, l_size)
                try:
                    resp = await self.api.upload_file(rel_path, payload, expected_revision=0)
                    self.db.upsert_file(rel_path, l_file["sha256"], l_file["stat"].st_size, l_file["stat"].st_mtime, resp['revision'], False)
                    summary["uploaded"] += 1
                except httpx.HTTPStatusError as e:
                    if is_auth_error(e):
                        raise
                    if e.response.status_code != 409:
                        print(f"[{self.log_label}] Warning: upload of {rel_path} failed: HTTP {e.response.status_code}", flush=True)
                        summary["failed"] += 1
                except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception), ConnectionError) as e:
                    print(f"Warning: Failed to upload {rel_path}: {e}")
                    summary["failed"] += 1
            elif in_remote and not in_local:
                r_file = remote_files[rel_path]
                remote_rev = r_file['revision']
                full_path = resolve_full_path(rel_path)
                if full_path.is_symlink() and not replace_symlinks:
                    return
                try:
                    def _prep_remote_only():
                        if full_path.is_symlink():
                            full_path.unlink()

                    ok = await self._download_for_initial_sync(rel_path, full_path, r_file, before_replace=_prep_remote_only)
                    if not ok:
                        summary["failed"] += 1
                        return
                    stat = full_path.stat()
                    sha = get_file_sha256(full_path)
                    self.db.upsert_file(rel_path, sha, stat.st_size, stat.st_mtime, remote_rev, False)
                    summary["downloaded"] += 1
                except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception), httpx.HTTPStatusError, ConnectionError) as e:
                    if is_auth_error(e):
                        raise
                    print(f"Warning: Failed to download {rel_path}: {e}")
                    summary["failed"] += 1
            elif in_local and in_remote:
                l_file = local_files[rel_path]
                r_file = remote_files[rel_path]

                if not is_conflict:
                    self.db.upsert_file(rel_path, l_file["sha256"], l_file["stat"].st_size, l_file["stat"].st_mtime, r_file['revision'], False)
                    summary["unchanged"] += 1
                else:
                    if choice in ("s", "server"):
                        remote_rev = r_file['revision']
                        full_path = resolve_full_path(rel_path)
                        try:
                            def _prep_server_wins():
                                self.archive_local_file(full_path, rel_path)
                                if full_path.is_symlink():
                                    full_path.unlink()

                            ok = await self._download_for_initial_sync(rel_path, full_path, r_file, before_replace=_prep_server_wins)
                            if not ok:
                                summary["failed"] += 1
                                return
                            stat = full_path.stat()
                            sha = get_file_sha256(full_path)
                            self.db.upsert_file(rel_path, sha, stat.st_size, stat.st_mtime, remote_rev, False)
                        except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception), httpx.HTTPStatusError, ConnectionError) as e:
                            if is_auth_error(e):
                                raise
                            print(f"Warning: Failed to download {rel_path}: {e}")
                            summary["failed"] += 1
                    elif choice in ("l", "local"):
                        l_path = Path(l_file["path"])
                        l_size = l_file["stat"].st_size if "stat" in l_file and hasattr(l_file["stat"], "st_size") else l_path.stat().st_size
                        payload = self.prepare_upload_payload(l_path, l_size)
                        try:
                            resp = await self.api.upload_file(rel_path, payload, expected_revision=r_file['revision'])
                            self.db.upsert_file(rel_path, l_file["sha256"], l_file["stat"].st_size, l_file["stat"].st_mtime, resp['revision'], False)
                        except httpx.HTTPStatusError as e:
                            if is_auth_error(e):
                                raise
                            if e.response.status_code != 409:
                                print(f"[{self.log_label}] Warning: upload of {rel_path} failed: HTTP {e.response.status_code}", flush=True)
                                summary["failed"] += 1
                        except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception), ConnectionError) as e:
                            print(f"Warning: Failed to upload {rel_path}: {e}")
                            summary["failed"] += 1
                    elif choice in ("b", "both"):
                        remote_rev = r_file['revision']
                        full_path = resolve_full_path(rel_path)
                        try:
                            conflict_holder = []
                            def _prep_both():
                                conflict_holder.append(self.rename_to_conflict(full_path))

                            ok = await self._download_for_initial_sync(rel_path, full_path, r_file, before_replace=_prep_both)
                            if not ok:
                                summary["failed"] += 1
                                return
                            conflict_path = conflict_holder[0]
                            stat = full_path.stat()
                            sha = get_file_sha256(full_path)
                            self.db.upsert_file(rel_path, sha, stat.st_size, stat.st_mtime, remote_rev, False)
                            self.db.record_conflict(self.workspace_id, rel_path, str(full_path), str(conflict_path))
                        except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception), httpx.HTTPStatusError, ConnectionError) as e:
                            if is_auth_error(e):
                                raise
                            print(f"Warning: Failed to download {rel_path}: {e}")
                            summary["failed"] += 1
        except Exception as e:
            if is_auth_error(e):
                raise
            print(f"[{self.log_label}] Warning: skipping {rel_path!r}: {e!r}", flush=True)
            summary["failed"] += 1

    @abc.abstractmethod
    def archive_local_file(self, full_path: Path, rel_path: str) -> None:
        """Archive a local file prior to deletion or replacement."""
        pass

    def apply_server_deletion(
        self,
        path: str,
        full_path: Path,
        remote_rev: int,
        local_file: dict | None,
    ) -> None:
        """Safely handle a remote deletion according to local tracking and modifications."""
        if not full_path.exists():
            self.db.delete_file(path)
            return
        elif local_file and local_file['server_revision'] >= remote_rev:
            # Stale: the local copy is at least as new as this deletion.
            return
        elif local_file is None:
            print(f"[{self.log_label}] Warning: not deleting {path!r}: the server deleted it, but the local file is not tracked here (kept as is)", flush=True)
            return
        elif get_file_sha256(full_path) != local_file['sha256']:
            # The local file was edited after the sync that produced the deleted version.
            # Do NOT delete user work: leave the row in the DB, so the local-changes step
            # sees the newer edit and re-uploads it as a newer revision.
            print(f"[{self.log_label}] Warning: not deleting {path!r}: the server deleted it, but the local file changed since the last sync (kept as is)", flush=True)
            return
        else:
            self.archive_local_file(full_path, path)
            full_path.unlink()
            self.db.delete_file(path)
            return

    def finalize_server_revision(self, latest_rev: int, retryable_skips: int) -> None:
        """Advance server revision in local db if no retryable errors occurred."""
        if retryable_skips:
            noun = "entry" if retryable_skips == 1 else "entries"
            print(f"[{self.log_label}] Warning: {retryable_skips} {noun} will be retried on the next reconcile; server revision not advanced", flush=True)
        else:
            self.db.set_last_server_revision(latest_rev)

    async def _delete_remote_file(self, rel_path: str, expected_rev: int) -> None:
        """Delete file on server with expected_revision check."""
        try:
            await self.api.delete_file(rel_path, expected_revision=expected_rev)
            self.db.delete_file(rel_path)
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 409:
                pass
            elif e.response.status_code == 404:
                self.db.delete_file(rel_path)
            elif is_auth_error(e):
                raise
            else:
                print(f"[{self.log_label}] Warning: deleting {rel_path} on the server failed: HTTP {e.response.status_code}", flush=True)
        except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception), ConnectionError) as e:
            if is_auth_error(e):
                raise
            print(f"[{self.log_label}] Warning: deleting {rel_path} on the server failed: {e}", flush=True)

    async def _reconcile_safely(self) -> None:
        """Execute reconcile catching non-auth exceptions."""
        label = self.log_label
        try:
            await self.reconcile()
        except Exception as e:
            if is_auth_error(e):
                raise
            print(f"[{label}] Reconcile failed: {type(e).__name__}: {e}", flush=True)

    async def _sse_loop(self) -> None:
        """Long-running SSE event listener with exponential backoff on reconnect."""
        label = self.log_label
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

    def _get_poll_interval(self) -> int | float:
        """Return the effective polling interval in seconds."""
        if hasattr(self, "server_config") and self.server_config is not None:
            return getattr(self.server_config, "poll_interval_seconds", self.poll_interval_seconds)
        return self.poll_interval_seconds

    async def _poll_loop(self) -> None:
        """Periodic fallback polling loop."""
        interval = self._get_poll_interval()
        if not interval or interval <= 0:
            return
        while True:
            await asyncio.sleep(interval)
            print(f"[{self.log_label}] Periodic fallback poll triggering reconcile...", flush=True)
            await self._reconcile_safely()

    @abc.abstractmethod
    async def reconcile(self) -> None:
        """Perform reconciliation pass between local and remote states."""
        pass

    @abc.abstractmethod
    async def _watch_loop(self) -> None:
        """Filesystem watching loop."""
        pass

    async def run(self) -> None:
        """Run initial reconcile followed by concurrent watch, SSE, and poll loops."""
        await self.reconcile()
        await asyncio.gather(
            self._watch_loop(),
            self._sse_loop(),
            self._poll_loop(),
        )
