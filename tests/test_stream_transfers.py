import pytest
import inspect
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock
from client.api import APIClient

@pytest.mark.asyncio
async def test_upload_file_streams_path_without_buffering(tmp_path: Path):
    """Criterion: APIClient.upload_file streams file chunks instead of loading all bytes into RAM."""
    client = APIClient("http://localhost:8000", "test-token", "client-id")
    test_file = tmp_path / "large.bin"
    expected_data = b"A" * 1024 * 128
    test_file.write_bytes(expected_data)

    collected_chunks = []
    async def fake_put(url, headers=None, content=None):
        if inspect.isasyncgen(content):
            async for chunk in content:
                collected_chunks.append(chunk)
        mock_resp = MagicMock(status_code=200, json=lambda: {"revision": 1})
        mock_resp.raise_for_status = MagicMock()
        return mock_resp

    with patch.object(client.client, "put", side_effect=fake_put):
        result = await client.upload_file("large.bin", test_file, expected_revision=5)
        assert result["revision"] == 1
        assert b"".join(collected_chunks) == expected_data
        assert len(collected_chunks) > 1


@pytest.mark.asyncio
async def test_download_file_to_path_streams_to_disk(tmp_path: Path):
    """Criterion: APIClient.download_file_to_path streams response chunks directly to disk."""
    client = APIClient("http://localhost:8000", "test-token", "client-id")
    target_file = tmp_path / "downloaded.bin"

    class MockStreamResponse:
        status_code = 200
        def raise_for_status(self):
            pass
        async def aiter_bytes(self):
            yield b"chunk1"
            yield b"chunk2"

    class MockStreamContext:
        async def __aenter__(self):
            return MockStreamResponse()
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_stream = MagicMock(return_value=MockStreamContext())

    with patch.object(client.client, "stream", mock_stream):
        await client.download_file_to_path("downloaded.bin", target_file)
        assert target_file.exists()
        assert target_file.read_bytes() == b"chunk1chunk2"


@pytest.mark.asyncio
async def test_download_file_to_path_failure_preserves_target(tmp_path: Path):
    """Criterion: If stream fails mid-transfer, original target is preserved and part files are removed."""
    import httpx
    client = APIClient("http://localhost:8000", "test-token", "client-id")
    target_file = tmp_path / "important.bin"
    target_file.write_bytes(b"original content")

    class FailingStreamResponse:
        status_code = 200
        def raise_for_status(self):
            pass
        async def aiter_bytes(self):
            yield b"partial data"
            raise httpx.TransportError("Network died mid-stream")

    class FailingStreamContext:
        async def __aenter__(self):
            return FailingStreamResponse()
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_stream = MagicMock(return_value=FailingStreamContext())

    with patch("asyncio.sleep", AsyncMock()):
        with patch.object(client.client, "stream", mock_stream):
            with pytest.raises(httpx.TransportError):
                await client.download_file_to_path("important.bin", target_file)

    # Target must remain intact
    assert target_file.read_bytes() == b"original content"
    # No .part files left in directory
    part_files = [f for f in tmp_path.iterdir() if ".part-" in f.name]
    assert part_files == []


@pytest.mark.asyncio
async def test_sync_engine_uploads_large_file_as_path(tmp_path: Path):
    """Criterion: SyncEngine passes Path to upload_file for files > STREAM_THRESHOLD."""
    from client.engine import SyncEngine
    from client.db import LocalDB
    from client.api import STREAM_THRESHOLD

    db = LocalDB(tmp_path / "db.sqlite")
    db.set_initialized(True)
    db.set_last_server_revision(0)
    api = AsyncMock()
    api.get_state.return_value = {"files": [], "last_revision": 0}
    api.upload_file.return_value = {"revision": 1}

    engine = SyncEngine(tmp_path, db, api)

    large_file = tmp_path / "large.txt"
    large_file.write_bytes(b"X" * (STREAM_THRESHOLD + 100))

    small_file = tmp_path / "small.txt"
    small_file.write_bytes(b"small")

    await engine.reconcile()

    # Verify upload_file was called with Path for large_file and bytes for small_file
    calls = {call[0][0]: call[0][1] for call in api.upload_file.call_args_list}
    assert isinstance(calls["large.txt"], Path)
    assert isinstance(calls["small.txt"], bytes)


@pytest.mark.asyncio
async def test_download_file_to_path_verifies_sha256(tmp_path: Path):
    import hashlib
    client = APIClient("http://localhost:8000", "test-token", "client-id")
    target_file = tmp_path / "downloaded.bin"
    payload = b"chunk1chunk2"
    good_sha = hashlib.sha256(payload).hexdigest()

    class MockStreamResponse:
        status_code = 200
        def raise_for_status(self):
            pass
        async def aiter_bytes(self):
            yield b"chunk1"
            yield b"chunk2"

    class MockStreamContext:
        async def __aenter__(self):
            return MockStreamResponse()
        async def __aexit__(self, exc_type, exc_val, exc_tb):
            pass

    mock_stream = MagicMock(return_value=MockStreamContext())

    with patch.object(client.client, "stream", mock_stream):
        with pytest.raises(ValueError, match="Hash mismatch"):
            await client.download_file_to_path("downloaded.bin", target_file, expected_sha256="0" * 64)
        assert not target_file.exists()

        digest = await client.download_file_to_path("downloaded.bin", target_file, expected_sha256=good_sha)
        assert digest == good_sha
        assert target_file.read_bytes() == payload
    await client.close()


@pytest.mark.asyncio
async def test_sync_engine_streams_large_download_to_disk(tmp_path: Path):
    import hashlib
    from client.engine import SyncEngine
    from client.db import LocalDB
    from client.api import STREAM_THRESHOLD

    db = LocalDB(tmp_path / ".syncme" / "db.sqlite")
    db.set_initialized(True)
    api = APIClient("http://localhost:8000", "test-token", "client-id")

    large_payload = b"Z" * (STREAM_THRESHOLD + 50)
    large_sha = hashlib.sha256(large_payload).hexdigest()

    api.get_state = AsyncMock(return_value={
        "files": [{"path": "big.bin", "revision": 2, "size": len(large_payload), "sha256": large_sha, "is_deleted": False}],
        "latest_revision": 2,
    })

    streamed = False
    orig_download_to_path = api.download_file_to_path
    async def spy_download_to_path(path, target, expected_sha256=None):
        nonlocal streamed
        streamed = True
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(large_payload)
        return large_sha

    with patch.object(APIClient, "download_file_to_path", side_effect=spy_download_to_path):
        engine = SyncEngine(tmp_path, db, api)
        await engine.reconcile()

    assert streamed is True
    assert (tmp_path / "big.bin").read_bytes() == large_payload
    await api.close()
    db.close()


@pytest.mark.asyncio
async def test_initial_sync_streams_large_download_to_disk(tmp_path: Path):
    import hashlib
    from client.engine import SyncEngine
    from client.db import LocalDB
    from client.api import STREAM_THRESHOLD

    db = LocalDB(tmp_path / ".syncme" / "db.sqlite")
    api = APIClient("http://localhost:8000", "test-token", "client-id")

    large_payload = b"Y" * (STREAM_THRESHOLD + 50)
    large_sha = hashlib.sha256(large_payload).hexdigest()

    api.get_state = AsyncMock(return_value={
        "files": [{"path": "big_init.bin", "revision": 1, "size": len(large_payload), "sha256": large_sha, "is_deleted": False}],
        "latest_revision": 1,
    })

    streamed = False
    async def spy_download_to_path(path, target, expected_sha256=None):
        nonlocal streamed
        streamed = True
        assert target.name.startswith(".syncme-tmp-")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(large_payload)
        return large_sha

    with patch.object(APIClient, "download_file_to_path", side_effect=spy_download_to_path):
        engine = SyncEngine(tmp_path, db, api)
        summary = await engine.initial_sync()

    assert streamed is True
    assert summary["downloaded"] == 1
    assert (tmp_path / "big_init.bin").read_bytes() == large_payload
    await api.close()
    db.close()


def test_atomic_write_respects_umask_for_new_files(tmp_path: Path):
    import os
    import stat as stat_module
    from client.base_engine import BaseEngine

    umask = os.umask(0o022)
    os.umask(umask)
    expected_mode = 0o666 & ~umask

    f = tmp_path / "new_file.txt"
    BaseEngine._atomic_write_bytes(f, b"hello")
    actual_mode = stat_module.S_IMODE(f.stat().st_mode)
    assert actual_mode == expected_mode
