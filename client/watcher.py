import time
import asyncio
import threading
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from typing import Callable, Set, Tuple
import os

from client.ignore import is_ignored

class SyncEventHandler(FileSystemEventHandler):
    def __init__(self, sync_dir: Path, callback: Callable[[str, str], None], replace_symlinks: bool = False):
        self.sync_dir = sync_dir
        self.watch_dir = self.sync_dir.resolve()
        self.callback = callback
        self.replace_symlinks = replace_symlinks

    def _get_rel_path(self, path: str) -> str:
        p = Path(path)
        try:
            return str(p.relative_to(self.watch_dir))
        except ValueError:
            pass
        try:
            return str(p.relative_to(self.sync_dir))
        except ValueError:
            return ""

    def _handle_event(self, event):
        if event.is_directory:
            return

        event_type = event.event_type
        if event_type == "moved":
            src_rel = self._get_rel_path(event.src_path)
            dest_rel = self._get_rel_path(event.dest_path)

            src_valid = bool(src_rel and not is_ignored(self.sync_dir, src_rel) and (self.replace_symlinks or not os.path.islink(event.src_path)))
            dest_valid = bool(dest_rel and not is_ignored(self.sync_dir, dest_rel) and (self.replace_symlinks or not os.path.islink(event.dest_path)))

            if src_valid and dest_valid:
                self.callback(src_rel, "deleted")
                self.callback(dest_rel, "modified")
            elif src_valid and not dest_valid:
                self.callback(src_rel, "deleted")
            elif not src_valid and dest_valid:
                self.callback(dest_rel, "modified")
            return

        if not self.replace_symlinks and os.path.islink(event.src_path):
            return
        rel_path = self._get_rel_path(event.src_path)
        if not rel_path or is_ignored(self.sync_dir, rel_path):
            return

        if event_type in ("modified", "created"):
            self.callback(rel_path, "modified")
        elif event_type == "deleted":
            self.callback(rel_path, "deleted")

    def on_created(self, event):
        self._handle_event(event)
    def on_modified(self, event):
        self._handle_event(event)
    def on_deleted(self, event):
        self._handle_event(event)
    def on_moved(self, event):
        self._handle_event(event)

class Watcher:
    def __init__(self, sync_dir: Path, debounce_seconds: float = 1.0, replace_symlinks: bool = False,
                 recursive: bool = True):
        self.sync_dir = sync_dir
        self.watch_dir = self.sync_dir.resolve()
        self.debounce_seconds = debounce_seconds
        self.replace_symlinks = replace_symlinks
        self.recursive = recursive
        self.observer = Observer()
        self.events: Set[Tuple[str, str]] = set()
        self._lock = threading.Lock()
        self._loop = None
        self._event_queue = asyncio.Queue()

    def _on_event(self, rel_path: str, event_type: str):
        print(f"[Watcher] Event detected: {event_type} on {rel_path}", flush=True)
        with self._lock:
            self.events.add((rel_path, event_type))
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._event_queue.put_nowait, (rel_path, event_type))

    def start(self, loop=None):
        if loop is not None:
            self._loop = loop
        else:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = None
        handler = SyncEventHandler(self.sync_dir, self._on_event, self.replace_symlinks)
        self.observer.schedule(handler, str(self.watch_dir), recursive=self.recursive)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        # Avoid blocking the event loop thread synchronously with an unbounded join:
        # self.observer.join(timeout=1.0)

    async def get_batches(self):
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass
        with self._lock:
            if self.events and self._event_queue.empty():
                for item in self.events:
                    self._event_queue.put_nowait(item)
        while True:
            # Wait until at least one event arrives (zero idle CPU usage)
            first = await self._event_queue.get()
            batch = {first}
            start_time = time.monotonic()
            max_wait = max(self.debounce_seconds * 5, 2.0)
            while True:
                elapsed = time.monotonic() - start_time
                if elapsed >= max_wait:
                    break
                remaining = min(self.debounce_seconds, max_wait - elapsed)
                try:
                    item = await asyncio.wait_for(self._event_queue.get(), timeout=remaining)
                    batch.add(item)
                except asyncio.TimeoutError:
                    break
            with self._lock:
                if self.events:
                    batch.update(self.events)
                    self.events.clear()
            yield batch

    async def get_events(self):
        async for batch in self.get_batches():
            for item in batch:
                yield item
