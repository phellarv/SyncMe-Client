"""Single-file entries must not make the client watch whole directory trees.

A file entry like ~/.bashrc used to start a recursive watcher on its parent (~), one inotify watch
per directory under the home directory, once per entry. That exhausted fs.inotify.max_user_watches
and crashed the daemon with "inotify watch limit reached". File entries now watch only their
parent directory itself, once per distinct parent.
"""
import asyncio
from unittest.mock import AsyncMock

import pytest

import client.file_engine as file_engine_module
from client.config import FileConfig, ServerConfig
from client.db import LocalDB
from client.file_engine import FileGroupEngine
from client.watcher import Watcher


@pytest.mark.asyncio
async def test_non_recursive_watcher_ignores_subdirectories(tmp_path):
    sub = tmp_path / "sub" / "deeper"
    sub.mkdir(parents=True)
    watcher = Watcher(tmp_path, debounce_seconds=0.1, recursive=False)
    watcher.start()
    try:
        (sub / "nested.txt").write_text("x")
        (tmp_path / "top.txt").write_text("x")

        async def first_batch():
            async for batch in watcher.get_batches():
                return batch

        batch = await asyncio.wait_for(first_batch(), timeout=2.0)
        paths = {rel_path for rel_path, _ in batch}
        assert "top.txt" in paths
        assert not any("nested.txt" in p for p in paths)
    finally:
        watcher.stop()


@pytest.mark.asyncio
async def test_recursive_stays_the_default(tmp_path):
    sub = tmp_path / "sub"
    sub.mkdir()
    watcher = Watcher(tmp_path, debounce_seconds=0.1)
    watcher.start()
    try:
        (sub / "nested.txt").write_text("x")

        async def first_batch():
            async for batch in watcher.get_batches():
                return batch

        batch = await asyncio.wait_for(first_batch(), timeout=2.0)
        assert any("nested.txt" in rel_path for rel_path, _ in batch)
    finally:
        watcher.stop()


class RecordingWatcher:
    created = []

    def __init__(self, sync_dir, debounce_seconds=1.0, replace_symlinks=False, recursive=True):
        self.sync_dir = sync_dir
        self.recursive = recursive
        type(self).created.append(self)

    def start(self, loop=None):
        pass

    def stop(self):
        pass

    async def get_batches(self):
        return
        yield


def make_engine(tmp_path, names):
    files = [FileConfig(local_path=tmp_path / name, workspace="ws", remote_path=name) for name in names]
    return FileGroupEngine("ws", files, LocalDB(tmp_path / "ws.db"), AsyncMock(), ServerConfig())


@pytest.mark.asyncio
async def test_file_entries_watch_their_parent_non_recursively(tmp_path, monkeypatch):
    RecordingWatcher.created = []
    monkeypatch.setattr(file_engine_module, "Watcher", RecordingWatcher)
    engine = make_engine(tmp_path, ["a.txt"])

    await asyncio.wait_for(engine._watch_loop(), 5)

    assert [w.recursive for w in RecordingWatcher.created] == [False]


@pytest.mark.asyncio
async def test_file_entries_in_one_directory_share_one_watcher(tmp_path, monkeypatch):
    RecordingWatcher.created = []
    monkeypatch.setattr(file_engine_module, "Watcher", RecordingWatcher)
    (tmp_path / "other").mkdir()
    engine = make_engine(tmp_path, ["a.txt", "b.txt", "c.txt", "other/d.txt"])

    await asyncio.wait_for(engine._watch_loop(), 5)

    assert sorted(w.sync_dir for w in RecordingWatcher.created) == [tmp_path, tmp_path / "other"]


@pytest.mark.asyncio
async def test_watch_loop_creates_missing_parent_directory(tmp_path):
    missing_dir = tmp_path / "nonexistent_parent"
    assert not missing_dir.exists()
    engine = make_engine(tmp_path, ["nonexistent_parent/config.fish"])

    task = asyncio.create_task(engine._watch_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert missing_dir.exists()


@pytest.mark.asyncio
async def test_watch_loop_survives_uncreatable_parent_directory(tmp_path):
    file_as_dir = tmp_path / "blocked_file"
    file_as_dir.write_text("i am a file")
    valid_dir = tmp_path / "valid_parent"
    valid_dir.mkdir()

    engine = make_engine(tmp_path, ["blocked_file/child.txt", "valid_parent/good.txt"])

    task = asyncio.create_task(engine._watch_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

