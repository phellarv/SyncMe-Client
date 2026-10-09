import pytest
import httpx
from unittest.mock import AsyncMock, patch, MagicMock
from client.api import APIClient

@pytest.mark.asyncio
async def test_get_history():
    client = APIClient("http://localhost:8000", "test-token", "client-id")
    mock_get = AsyncMock()
    mock_get.return_value = MagicMock(status_code=200, json=lambda: {"history": [{"revision": 1, "size": 100}]})
    mock_get.return_value.raise_for_status = MagicMock()
    
    with patch.object(client.client, "get", mock_get):
        result = await client.get_history("test.txt", limit=50)
        assert "history" in result
        assert len(result["history"]) == 1
        mock_get.assert_called_once_with("http://localhost:8000/api/v1/workspaces/default/history/test.txt", params={"limit": 50})

@pytest.mark.asyncio
async def test_get_history_revision():
    client = APIClient("http://localhost:8000", "test-token", "client-id")
    mock_get = AsyncMock()
    mock_get.return_value = MagicMock(status_code=200, content=b"test content")
    mock_get.return_value.raise_for_status = MagicMock()
    
    with patch.object(client.client, "get", mock_get):
        result = await client.get_history_revision("test.txt", 1)
        assert result == b"test content"
        mock_get.assert_called_once_with("http://localhost:8000/api/v1/workspaces/default/revisions/1/test.txt")

@pytest.mark.asyncio
async def test_restore_history():
    client = APIClient("http://localhost:8000", "test-token", "client-id")
    mock_post = AsyncMock()
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"message": "restored", "new_revision": 2})
    mock_post.return_value.raise_for_status = MagicMock()
    
    with patch.object(client.client, "post", mock_post):
        result = await client.restore_history("test.txt", 1)
        assert result["message"] == "restored"
        mock_post.assert_called_once_with(
            "http://localhost:8000/api/v1/workspaces/default/history/test.txt/restore",
            json={"restore_revision": 1}
        )

@pytest.mark.asyncio
async def test_restore_history_with_expected():
    client = APIClient("http://localhost:8000", "test-token", "client-id")
    mock_post = AsyncMock()
    mock_post.return_value = MagicMock(status_code=200, json=lambda: {"message": "restored", "new_revision": 2})
    mock_post.return_value.raise_for_status = MagicMock()
    
    with patch.object(client.client, "post", mock_post):
        result = await client.restore_history("test.txt", 1, expected_revision=5)
        assert result["message"] == "restored"
        mock_post.assert_called_once_with(
            "http://localhost:8000/api/v1/workspaces/default/history/test.txt/restore",
            json={"restore_revision": 1, "expected_revision": 5}
        )

@pytest.mark.asyncio
async def test_get_history_404():
    client = APIClient("http://localhost:8000", "test-token", "client-id")
    mock_get = AsyncMock()
    mock_get.return_value = MagicMock(status_code=404)
    mock_get.return_value.raise_for_status.side_effect = httpx.HTTPStatusError("404 Not Found", request=MagicMock(), response=mock_get.return_value)
    
    with patch.object(client.client, "get", mock_get):
        with pytest.raises(httpx.HTTPStatusError):
            await client.get_history("test.txt")
