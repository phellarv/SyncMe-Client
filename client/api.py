import asyncio
import httpx
import json
from pathlib import Path
from typing import AsyncGenerator, Dict, Any, Optional, Union

class APIClient:
    def __init__(self, server_url: str, api_key: str, client_id: str, workspace_id: str = "default", timeout: float = 300.0):
        self.server_url = server_url.rstrip("/")
        self.api_key = api_key
        self.client_id = client_id
        self.workspace_id = workspace_id
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "X-Client-ID": self.client_id
        }
        self.client = httpx.AsyncClient(headers=self.headers, timeout=httpx.Timeout(connect=15.0, read=timeout, write=timeout, pool=15.0))

    async def get_health(self) -> Dict[str, Any]:
        resp = await self.client.get(f"{self.server_url}/api/v1/health")
        resp.raise_for_status()
        return resp.json()

    async def get_state(self, since_revision: int = 0) -> Dict[str, Any]:
        resp = await self.client.get(f"{self.server_url}/api/v1/workspaces/{self.workspace_id}/sync/state", params={"since_revision": since_revision})
        resp.raise_for_status()
        return resp.json()

    async def download_file(self, path: str) -> bytes:
        for attempt in range(3):
            try:
                resp = await self.client.get(f"{self.server_url}/api/v1/workspaces/{self.workspace_id}/files/{path}")
                resp.raise_for_status()
                return resp.content
            except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception)) as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(1.0 * (2 ** attempt))

    async def upload_file(self, path: str, content: Union[bytes, Path], expected_revision: Optional[int] = None) -> Dict[str, Any]:
        if isinstance(content, Path):
            with open(content, "rb") as f:
                content = f.read()
        headers = {}
        if expected_revision is not None:
            headers["If-Match"] = str(expected_revision)
        for attempt in range(3):
            try:
                resp = await self.client.put(f"{self.server_url}/api/v1/workspaces/{self.workspace_id}/files/{path}", headers=headers, content=content)
                resp.raise_for_status()
                return resp.json()
            except (httpx.TimeoutException, getattr(httpx, "TransportError", Exception), getattr(httpx, "NetworkError", Exception)) as e:
                if attempt == 2:
                    raise
                await asyncio.sleep(1.0 * (2 ** attempt))

    async def delete_file(self, path: str, expected_revision: Optional[int] = None) -> Dict[str, Any]:
        headers = {}
        if expected_revision is not None:
            headers["If-Match"] = str(expected_revision)
        resp = await self.client.delete(f"{self.server_url}/api/v1/workspaces/{self.workspace_id}/files/{path}", headers=headers)
        resp.raise_for_status()
        return resp.json()

    async def listen_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        async with self.client.stream("GET", f"{self.server_url}/api/v1/workspaces/{self.workspace_id}/events",
                                      # the server pings every 15 s, so 60 s of silence means a dead connection
                                      timeout=httpx.Timeout(connect=15.0, read=60.0, write=15.0, pool=15.0)) as response:
            response.raise_for_status()
            # The stream is open: tell the caller first, so it can reconcile now (events in the
            # gap before this connection are not replayed) and measure how long the stream lives.
            yield {"event": "connected"}
            async for line in response.aiter_lines():
                line = line.strip()
                if line.startswith("data: "):
                    data_str = line[6:]
                    try:
                        yield json.loads(data_str)
                    except json.JSONDecodeError:
                        pass
    
    async def close(self):
        await self.client.aclose()

    async def get_history(self, path: str, limit: int = 50) -> Dict[str, Any]:
        import urllib.parse
        resp = await self.client.get(
            f"{self.server_url}/api/v1/workspaces/{self.workspace_id}/history/{urllib.parse.quote(path)}",
            params={"limit": limit}
        )
        resp.raise_for_status()
        return resp.json()

    async def get_history_revision(self, path: str, revision: int) -> bytes:
        import urllib.parse
        resp = await self.client.get(
            f"{self.server_url}/api/v1/workspaces/{self.workspace_id}/revisions/{revision}/{urllib.parse.quote(path)}"
        )
        resp.raise_for_status()
        return resp.content

    async def restore_history(self, path: str, restore_revision: int, expected_revision: Optional[int] = None) -> Dict[str, Any]:
        import urllib.parse
        payload = {"restore_revision": restore_revision}
        if expected_revision is not None:
            payload["expected_revision"] = expected_revision
            
        resp = await self.client.post(
            f"{self.server_url}/api/v1/workspaces/{self.workspace_id}/history/{urllib.parse.quote(path)}/restore",
            json=payload
        )
        resp.raise_for_status()
        return resp.json()
