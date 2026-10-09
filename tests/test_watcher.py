import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch
import os
import shutil
import pytest

from client.watcher import Watcher
from client.engine import SyncEngine
from client.db import LocalDB


# Criterion 1: Watcher captures filesystem events and yields them via get_events()
@pytest.mark.asyncio
async def test_watcher_captures_filesystem_events(tmp_path: Path):
    """Watcher must capture filesystem modifications and yield them via get_events()."""
    watcher = Watcher(tmp_path, debounce_seconds=0.1)
    watcher.start()
    try:
        # Write file to trigger event
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello")

        async def get_first_event():
            async for rel_path, event_type in watcher.get_events():
                return rel_path, event_type

        # Watcher should yield ("test.txt", "modified") within 2.0s
        rel_path, event_type = await asyncio.wait_for(get_first_event(), timeout=2.0)
        assert rel_path == "test.txt"
        assert event_type == "modified"
    finally:
        watcher.stop()


# Criterion 2: SyncEngine periodically reconciles when poll_interval_seconds is configured
@pytest.mark.asyncio
async def test_engine_periodic_polling(tmp_path: Path):
    """SyncEngine must periodically call reconcile() during run() when poll_interval_seconds is set."""
    db = LocalDB(tmp_path / ".syncme" / "local_state.db")
    db.set_initialized(True)
    api = AsyncMock()
    api.get_state.return_value = {"files": [], "last_revision": 1}

    async def fake_listen_events():
        while True:
            await asyncio.sleep(10)
            yield {}

    api.listen_events = fake_listen_events

    # Set short poll_interval_seconds to 0.1s
    engine = SyncEngine(tmp_path, db, api, poll_interval_seconds=0.1)

    with patch.object(engine, "reconcile", new_callable=AsyncMock) as mock_reconcile:
        task = asyncio.create_task(engine.run())
        try:
            # Wait for 0.35s so that multiple poll intervals (at 0.1s) trigger
            await asyncio.sleep(0.35)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Should be called once initially + periodically during run (~4 calls total, assert at least 2)
        assert mock_reconcile.call_count >= 2, (
            f"Expected reconcile to be called at least 2 times, but was called {mock_reconcile.call_count} times"
        )

@pytest.mark.asyncio
async def test_watcher_symlink_dir(tmp_path: Path):
    """Watcher should handle being pointed at a symlink to a directory."""
    real_dir = tmp_path / "real_dir"
    real_dir.mkdir()
    symlink_dir = tmp_path / "symlink_dir"
    os.symlink(real_dir, symlink_dir)

    watcher = Watcher(symlink_dir, debounce_seconds=0.1)
    watcher.start()
    try:
        # Write file in the symlinked directory
        test_file = symlink_dir / "test.txt"
        test_file.write_text("hello")

        async def get_first_event():
            async for rel_path, event_type in watcher.get_events():
                return rel_path, event_type

        rel_path, event_type = await asyncio.wait_for(get_first_event(), timeout=2.0)
        assert rel_path == "test.txt"
        assert event_type == "modified"
    finally:
        watcher.stop()


@pytest.mark.asyncio
async def test_watcher_moved_events(tmp_path: Path):
    """Watcher should correctly handle 'moved' events (like atomic saves and moving out of tree)."""
    sync_dir = tmp_path / "sync_dir"
    sync_dir.mkdir()

    watcher = Watcher(sync_dir, debounce_seconds=0.1)
    watcher.start()
    try:
        # a) Atomic save: move from outside to inside
        outside_file = tmp_path / "outside.txt"
        outside_file.write_text("content")
        inside_file = sync_dir / "inside.txt"

        async def get_events_until(num_events):
            events = []
            async for rel_path, event_type in watcher.get_events():
                events.append((rel_path, event_type))
                if len(events) >= num_events:
                    return events
            return events

        # Start listening for the move event
        listen_task = asyncio.create_task(asyncio.wait_for(get_events_until(1), timeout=2.0))

        shutil.move(str(outside_file), str(inside_file))

        events = await listen_task
        assert ("inside.txt", "modified") in events

        # b) Move out of tree: move from inside to outside
        listen_task2 = asyncio.create_task(asyncio.wait_for(get_events_until(1), timeout=2.0))

        shutil.move(str(inside_file), str(outside_file))

        events2 = await listen_task2
        assert ("inside.txt", "deleted") in events2

    finally:
        watcher.stop()

@pytest.mark.asyncio
async def test_watcher_rename_internal(tmp_path: Path):
    """Watcher should correctly handle 'moved' events within the tree."""
    sync_dir = tmp_path / "sync_dir"
    sync_dir.mkdir()
    
    file_a = sync_dir / "a.txt"
    file_a.write_text("hello")
    
    watcher = Watcher(sync_dir, debounce_seconds=0.1)
    watcher.start()
    try:
        file_b = sync_dir / "b.txt"
        file_a.rename(file_b)
        
        events = set()
        async def collect_events():
            async for batch_events in watcher.get_batches():
                for item in batch_events:
                    events.add(item)
                if ("a.txt", "deleted") in events and ("b.txt", "modified") in events:
                    break

        await asyncio.wait_for(collect_events(), timeout=2.0)
        assert ("a.txt", "deleted") in events
        assert ("b.txt", "modified") in events
    finally:
        watcher.stop()

@pytest.mark.asyncio
async def test_watcher_atomic_rename_from_ignored(tmp_path: Path):
    """Watcher should handle atomic rename from ignored to valid file."""
    sync_dir = tmp_path / "sync_dir"
    sync_dir.mkdir()
    
    tmp_file = sync_dir / ".syncme-tmp-test"
    tmp_file.write_text("content")
    
    watcher = Watcher(sync_dir, debounce_seconds=0.1)
    watcher.start()
    try:
        target_file = sync_dir / "target.txt"
        tmp_file.rename(target_file)
        
        events = set()
        async def collect_events():
            async for batch_events in watcher.get_batches():
                for item in batch_events:
                    events.add(item)
                if ("target.txt", "modified") in events:
                    break

        await asyncio.wait_for(collect_events(), timeout=2.0)
        assert ("target.txt", "modified") in events
    finally:
        watcher.stop()
