import pytest
import os
import asyncio
from pathlib import Path
from unittest.mock import patch
from click.testing import CliRunner
import hashlib

from client.config import load_config
from client.cli import cli

class MockServer:
    def __init__(self):
        self.workspaces = {}

    def _get_ws(self, ws_id):
        if ws_id not in self.workspaces:
            self.workspaces[ws_id] = {"files": {}, "last_revision": 0}
        return self.workspaces[ws_id]

    def get_state(self, ws_id, since_revision=0):
        ws = self._get_ws(ws_id)
        files = []
        for path, info in ws["files"].items():
            if info["revision"] > since_revision:
                files.append(info)
        return {"files": files, "last_revision": ws["last_revision"]}
    
    def upload_file(self, ws_id, path, content, expected_revision=None):
        ws = self._get_ws(ws_id)
        existing = ws["files"].get(path)
        if expected_revision is not None:
            if expected_revision == 0 and existing and not existing.get("is_deleted", False):
                raise Exception("File exists")
            if existing and existing["revision"] != expected_revision:
                raise Exception("Revision mismatch")
        
        ws["last_revision"] += 1
        sha = hashlib.sha256(content).hexdigest()
        info = {
            "path": path,
            "revision": ws["last_revision"],
            "is_deleted": False,
            "sha256": sha,
            "_content": content
        }
        ws["files"][path] = info
        return {"revision": ws["last_revision"]}

    def download_file(self, ws_id, path):
        ws = self._get_ws(ws_id)
        return ws["files"][path]["_content"]

    def delete_file(self, ws_id, path, expected_revision=None):
        ws = self._get_ws(ws_id)
        ws["last_revision"] += 1
        if path in ws["files"]:
            ws["files"][path]["is_deleted"] = True
            ws["files"][path]["revision"] = ws["last_revision"]
        return {"revision": ws["last_revision"]}

def make_mock_api_client(mock_server):
    class FakeAPIClient:
        def __init__(self, server_url, api_key, client_id, workspace_id="default", **kwargs):
            self.workspace_id = workspace_id
            self.server = mock_server

        async def get_state(self, since_revision=0):
            return self.server.get_state(self.workspace_id, since_revision)

        async def download_file(self, path):
            return self.server.download_file(self.workspace_id, path)

        async def upload_file(self, path, content, expected_revision=None):
            return self.server.upload_file(self.workspace_id, path, content, expected_revision)

        async def delete_file(self, path, expected_revision=None):
            return self.server.delete_file(self.workspace_id, path, expected_revision)

        async def get_health(self):
            return {"status": "ok"}
            
        async def close(self):
            pass
            
    return FakeAPIClient

@pytest.fixture
def mock_server():
    return MockServer()

def test_e2e_multidir(tmp_path, mock_server):
    runner = CliRunner()
    
    docs_path = tmp_path / "docs"
    notes_path = tmp_path / "notes"
    docs_path.mkdir()
    notes_path.mkdir()
    
    config_path = tmp_path / "config.toml"
    
    # Note: Using forward slashes for cross-platform compatibility, though str(path) will use OS separator
    config_content = f"""
[server]
url = "http://localhost:8000"
api_key = "{'k' * 43}"

[[directories]]
name = "docs"
local_path = "{str(docs_path).replace(chr(92), '/')}"

[[directories]]
name = "notes"
local_path = "{str(notes_path).replace(chr(92), '/')}"
"""
    config_path.write_text(config_content)
    
    test_config = load_config(config_path)
    
    with patch("client.cli.settings", test_config), \
         patch("client.cli.APIClient", make_mock_api_client(mock_server)):
        
        # 1. Initialize both workspaces
        result = runner.invoke(cli, ["init"])
        assert result.exit_code == 0, f"Init failed: {result.output}"
        assert "[docs] Initial sync complete" in result.output
        assert "[notes] Initial sync complete" in result.output
        
        # 2. Create files
        guide_file = docs_path / "guide.txt"
        guide_file.write_text("Guide Content")
        
        memo_file = notes_path / "memo.txt"
        memo_file.write_text("Memo Content")
        
        # 3. Sync workspaces
        result = runner.invoke(cli, ["sync"])
        assert result.exit_code == 0, f"Sync failed: {result.output}"
        
        # 4. Check that docs has guide.txt on server and notes has memo.txt on server
        docs_state = mock_server.get_state("docs")
        docs_files = [f["path"] for f in docs_state["files"] if not f["is_deleted"]]
        assert "guide.txt" in docs_files
        assert "memo.txt" not in docs_files
        
        notes_state = mock_server.get_state("notes")
        notes_files = [f["path"] for f in notes_state["files"] if not f["is_deleted"]]
        assert "memo.txt" in notes_files
        assert "guide.txt" not in notes_files
        
        # 5. Check local isolation
        assert not (docs_path / "memo.txt").exists()
        assert not (notes_path / "guide.txt").exists()
        
        # 6. Status command output
        result = runner.invoke(cli, ["status"])
        assert "[docs] Tracking 1 files." in result.output
        assert "[notes] Tracking 1 files." in result.output
