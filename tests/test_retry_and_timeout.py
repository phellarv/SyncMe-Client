import pytest
import os
import httpx
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from pathlib import Path
from client.api import APIClient
from client.engine import SyncEngine
from client.config import load_config, ClientConfig

@pytest.mark.asyncio
async def test_api_client_retry_on_timeout():
    client = APIClient("http://test", "key", "client")
    mock_get = AsyncMock()
    # Fail twice, succeed on third
    mock_get.side_effect = [
        httpx.TimeoutException("Timeout 1"),
        httpx.TimeoutException("Timeout 2"),
        MagicMock(status_code=200, content=b"success", raise_for_status=MagicMock())
    ]
    
    with patch.object(client.client, "get", mock_get):
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            content = await client.download_file("test.txt")
            assert content == b"success"
            assert mock_get.call_count == 3
            assert mock_sleep.call_count == 2
            
@pytest.mark.asyncio
async def test_api_client_fails_after_retries():
    client = APIClient("http://test", "key", "client")
    mock_get = AsyncMock(side_effect=httpx.TimeoutException("Timeout"))
    
    with patch.object(client.client, "get", mock_get):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(httpx.TimeoutException):
                await client.download_file("test.txt")
            assert mock_get.call_count == 3

@pytest.mark.asyncio
async def test_initial_sync_resilience(tmp_path):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()
    
    (sync_dir / "file1.txt").write_text("local1")
    (sync_dir / "file2.txt").write_text("local2")
    
    db_mock = MagicMock()
    db_mock.get_all_files.return_value = []
    
    api_mock = MagicMock()
    api_mock.get_state = AsyncMock(return_value={"files": [], "last_revision": 0})
    
    # upload_file fails once (raises exception) and succeeds once
    async def mock_upload(path, content, expected_revision=None):
        if path == "file1.txt":
            raise httpx.TimeoutException("timeout")
        return {"revision": 1}
        
    api_mock.upload_file = AsyncMock(side_effect=mock_upload)
    
    engine = SyncEngine(sync_dir, db_mock, api_mock)
    
    summary = await engine.initial_sync()
    
    assert summary["uploaded"] == 1
    assert summary["failed"] == 1
    # Check that execution didn't crash and we continued

def test_request_timeout_config_loading(monkeypatch):
    monkeypatch.setenv("SYNCME_REQUEST_TIMEOUT", "123.4")
    config = load_config()
    assert config.REQUEST_TIMEOUT == 123.4
    assert config.server.request_timeout == 123.4
