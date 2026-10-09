"""
Unit tests for conflict queue and resolution feature (TDD Phase: Gate 2).
Spec / Acceptance Criteria:
1. LocalDB conflict methods in client.db:
   - record_conflict(workspace: str, rel_path: str, server_file: str, conflict_file: str) -> int
   - get_pending_conflicts(workspace: str = None) -> list[dict]
   - get_conflict(conflict_id: int) -> dict | None
   - resolve_conflict_record(conflict_id: int, resolution: str) -> None
   - dismiss_conflict(conflict_id: int) -> None
   - count_pending_conflicts(workspace: str = None) -> int
   - Duplicate pending conflict for the same rel_path updates/returns existing record.
2. ConflictResolver in client.conflicts:
   - resolve_conflict with action='local' (copies conflict file to server file, uploads to api, unlinks conflict file, marks resolved in db)
   - resolve_conflict with action='server' (unlinks conflict file, marks resolved in db, keeps server file)
   - resolve_conflict with action='meld' (runs meld command, handles user confirmation/returncode, uploads merged result if changed, unlinks conflict file, marks resolved)
3. CLI commands for conflicts in client.cli:
   - syncme-client conflicts list
   - syncme-client conflicts count (with --format waybar for JSON output)
   - syncme-client conflicts resolve (interactive prompt with 'l', 's', 'q')
"""

import asyncio
import inspect
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from client.cli import cli
from client.db import LocalDB
from client.conflicts import format_inline_diff
from client.conflicts import ConflictResolver


async def _execute_resolve(resolver, *args, **kwargs):
    res = resolver.resolve_conflict(*args, **kwargs)
    if inspect.iscoroutine(res):
        return await res
    return res


# ===========================================================================
# 1. LocalDB Conflict Queue Tests (client/db.py)
# ===========================================================================

class TestLocalDBConflictQueue:
    """Tests for conflict queue operations on LocalDB."""

    def test_record_and_get_conflict(self, tmp_path: Path):
        """Criterion: record_conflict adds a pending conflict and returns its ID; get_conflict retrieves it."""
        db = LocalDB(tmp_path / "test.db")
        conflict_id = db.record_conflict(
            workspace="default",
            rel_path="notes.txt",
            server_file=str(tmp_path / "notes.txt"),
            conflict_file=str(tmp_path / "notes.conflict.1.txt"),
        )
        assert isinstance(conflict_id, int)
        assert conflict_id > 0

        record = db.get_conflict(conflict_id)
        assert record is not None
        assert record["id"] == conflict_id
        assert record["workspace"] == "default"
        assert record["rel_path"] == "notes.txt"
        assert record["server_file"] == str(tmp_path / "notes.txt")
        assert record["conflict_file"] == str(tmp_path / "notes.conflict.1.txt")
        assert not record.get("resolved")
        db.close()

    def test_get_conflict_nonexistent(self, tmp_path: Path):
        """Criterion: get_conflict returns None for unknown conflict_id."""
        db = LocalDB(tmp_path / "test.db")
        assert db.get_conflict(99999) is None
        db.close()

    def test_get_pending_conflicts_all(self, tmp_path: Path):
        """Criterion: get_pending_conflicts() with no arguments returns all pending conflicts across workspaces."""
        db = LocalDB(tmp_path / "test.db")
        (tmp_path / "c1").touch()
        (tmp_path / "c2").touch()
        cid1 = db.record_conflict("ws1", "file1.txt", "/s1", str(tmp_path / "c1"))
        cid2 = db.record_conflict("ws2", "file2.txt", "/s2", str(tmp_path / "c2"))

        pending = db.get_pending_conflicts()
        pending_ids = [p["id"] for p in pending]
        assert cid1 in pending_ids
        assert cid2 in pending_ids
        assert len(pending) == 2
        db.close()

    def test_get_pending_conflicts_filtered_by_workspace(self, tmp_path: Path):
        """Criterion: get_pending_conflicts(workspace) filters records by workspace."""
        db = LocalDB(tmp_path / "test.db")
        (tmp_path / "c1").touch()
        (tmp_path / "c2").touch()
        cid1 = db.record_conflict("ws1", "file1.txt", "/s1", str(tmp_path / "c1"))
        cid2 = db.record_conflict("ws2", "file2.txt", "/s2", str(tmp_path / "c2"))

        ws1_pending = db.get_pending_conflicts(workspace="ws1")
        assert len(ws1_pending) == 1
        assert ws1_pending[0]["id"] == cid1

        ws2_pending = db.get_pending_conflicts(workspace="ws2")
        assert len(ws2_pending) == 1
        assert ws2_pending[0]["id"] == cid2
        db.close()

    def test_count_pending_conflicts(self, tmp_path: Path):
        """Criterion: count_pending_conflicts counts total or per-workspace pending conflicts."""
        db = LocalDB(tmp_path / "test.db")
        assert db.count_pending_conflicts() == 0

        (tmp_path / "c1").touch()
        (tmp_path / "c2").touch()
        (tmp_path / "c3").touch()
        db.record_conflict("ws1", "file1.txt", "/s1", str(tmp_path / "c1"))
        db.record_conflict("ws1", "file2.txt", "/s2", str(tmp_path / "c2"))
        db.record_conflict("ws2", "file3.txt", "/s3", str(tmp_path / "c3"))

        assert db.count_pending_conflicts() == 3
        assert db.count_pending_conflicts(workspace="ws1") == 2
        assert db.count_pending_conflicts(workspace="ws2") == 1
        assert db.count_pending_conflicts(workspace="unknown") == 0
        db.close()

    def test_resolve_conflict_record(self, tmp_path: Path):
        """Criterion: resolve_conflict_record marks record as resolved with resolution type and removes from pending."""
        db = LocalDB(tmp_path / "test.db")
        (tmp_path / "c").touch()
        cid = db.record_conflict("default", "file.txt", "/s", str(tmp_path / "c"))
        assert db.count_pending_conflicts() == 1

        db.resolve_conflict_record(cid, resolution="local")

        assert db.count_pending_conflicts() == 0
        assert len(db.get_pending_conflicts()) == 0

        record = db.get_conflict(cid)
        assert record is not None
        assert bool(record.get("resolved")) is True
        assert record.get("resolution") == "local"
        db.close()

    def test_dismiss_conflict(self, tmp_path: Path):
        """Criterion: dismiss_conflict removes the conflict from pending status."""
        db = LocalDB(tmp_path / "test.db")
        (tmp_path / "c").touch()
        cid = db.record_conflict("default", "file.txt", "/s", str(tmp_path / "c"))
        assert db.count_pending_conflicts() == 1

        db.dismiss_conflict(cid)

        assert db.count_pending_conflicts() == 0
        assert len(db.get_pending_conflicts()) == 0
        db.close()

    def test_duplicate_pending_conflict_updates_existing(self, tmp_path: Path):
        """Criterion: Duplicate pending conflict for same workspace and rel_path updates/returns existing record."""
        db = LocalDB(tmp_path / "test.db")
        (tmp_path / "c_old").touch()
        (tmp_path / "c_new").touch()
        cid1 = db.record_conflict("ws1", "file.txt", "/s_old", str(tmp_path / "c_old"))
        cid2 = db.record_conflict("ws1", "file.txt", "/s_new", str(tmp_path / "c_new"))

        assert cid1 == cid2
        assert db.count_pending_conflicts("ws1") == 1

        record = db.get_conflict(cid1)
        assert record["conflict_file"] == str(tmp_path / "c_new")
        assert record["server_file"] == "/s_new"
        db.close()

    def test_new_conflict_allowed_after_previous_resolved(self, tmp_path: Path):
        """Criterion: A new conflict for the same rel_path is allowed after the previous one was resolved."""
        db = LocalDB(tmp_path / "test.db")
        (tmp_path / "c1").touch()
        (tmp_path / "c2").touch()
        cid1 = db.record_conflict("ws1", "file.txt", "/s1", str(tmp_path / "c1"))
        db.resolve_conflict_record(cid1, resolution="server")

        cid2 = db.record_conflict("ws1", "file.txt", "/s2", str(tmp_path / "c2"))
        assert cid2 != cid1
        assert db.count_pending_conflicts("ws1") == 1
        db.close()


# ===========================================================================
# 2. ConflictResolver Tests (client/conflicts.py)
# ===========================================================================

class TestConflictResolver:
    """Tests for ConflictResolver resolution actions."""

    @pytest.fixture
    def setup_env(self, tmp_path: Path):
        db = LocalDB(tmp_path / "test.db")
        api = AsyncMock()
        api.upload_file.return_value = {"revision": 2}
        resolver = ConflictResolver(db=db, api=api)

        server_file = tmp_path / "doc.txt"
        conflict_file = tmp_path / "doc.conflict.123.txt"
        server_file.write_text("server version")
        conflict_file.write_text("local conflict version")

        conflict_id = db.record_conflict(
            workspace="default",
            rel_path="doc.txt",
            server_file=str(server_file),
            conflict_file=str(conflict_file),
        )

        return {
            "db": db,
            "api": api,
            "resolver": resolver,
            "server_file": server_file,
            "conflict_file": conflict_file,
            "conflict_id": conflict_id,
        }

    @pytest.mark.asyncio
    async def test_resolve_conflict_local_action(self, setup_env):
        """Criterion: action='local' copies conflict file to server file, uploads to api, unlinks conflict file, marks resolved."""
        env = setup_env
        resolver = env["resolver"]
        cid = env["conflict_id"]
        server_file = env["server_file"]
        conflict_file = env["conflict_file"]
        api = env["api"]
        db = env["db"]

        await _execute_resolve(resolver, conflict_id=cid, action="local")

        assert server_file.read_text() == "local conflict version"
        assert not conflict_file.exists()
        api.upload_file.assert_called()
        rec = db.get_conflict(cid)
        assert bool(rec["resolved"]) is True
        assert rec["resolution"] == "local"
        db.close()

    @pytest.mark.asyncio
    async def test_resolve_conflict_server_action(self, setup_env):
        """Criterion: action='server' keeps server file intact, unlinks conflict file, marks resolved, no upload."""
        env = setup_env
        resolver = env["resolver"]
        cid = env["conflict_id"]
        server_file = env["server_file"]
        conflict_file = env["conflict_file"]
        api = env["api"]
        db = env["db"]

        await _execute_resolve(resolver, conflict_id=cid, action="server")

        assert server_file.read_text() == "server version"
        assert not conflict_file.exists()
        api.upload_file.assert_not_called()
        rec = db.get_conflict(cid)
        assert bool(rec["resolved"]) is True
        assert rec["resolution"] == "server"
        db.close()

    @pytest.mark.asyncio
    async def test_resolve_conflict_meld_action(self, setup_env):
        """Criterion: action='meld' runs meld, on success uploads merged result, unlinks conflict file, marks resolved."""
        env = setup_env
        resolver = env["resolver"]
        cid = env["conflict_id"]
        server_file = env["server_file"]
        conflict_file = env["conflict_file"]
        api = env["api"]
        db = env["db"]

        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))
        mock_proc.wait = AsyncMock(return_value=0)

        def fake_run(*args, **kwargs):
            server_file.write_text("merged content from meld")
            return mock_proc

        with patch("asyncio.create_subprocess_exec", side_effect=fake_run) as mock_exec:
            await _execute_resolve(resolver, conflict_id=cid, action="meld")

        assert mock_exec.called
        call_args = [str(a) for a in mock_exec.call_args[0]]
        assert any("meld" in a for a in call_args)

        assert not conflict_file.exists()
        assert server_file.read_text() == "merged content from meld"
        api.upload_file.assert_called()
        rec = db.get_conflict(cid)
        assert bool(rec["resolved"]) is True
        assert rec["resolution"] == "meld"
        db.close()

    @pytest.mark.asyncio
    async def test_resolve_conflict_meld_aborted_keeps_conflict(self, setup_env):
        """Criterion: If meld command fails or is aborted (exit code != 0), files remain and conflict stays pending."""
        env = setup_env
        resolver = env["resolver"]
        cid = env["conflict_id"]
        server_file = env["server_file"]
        conflict_file = env["conflict_file"]
        api = env["api"]
        db = env["db"]

        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.wait = AsyncMock(return_value=1)
        mock_proc.communicate = AsyncMock(return_value=(b"", b"meld aborted"))

        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            try:
                await _execute_resolve(resolver, conflict_id=cid, action="meld")
            except Exception:
                pass

        assert server_file.exists()
        assert conflict_file.exists()
        api.upload_file.assert_not_called()
        rec = db.get_conflict(cid)
        assert not rec.get("resolved")
        db.close()


# ===========================================================================
# 3. CLI Command Tests (client/cli.py)
# ===========================================================================

class TestCLIConflicts:
    """Tests for 'syncme-client conflicts' subcommands using Click CliRunner."""

    def test_cli_conflicts_list_empty(self, tmp_path: Path):
        """Criterion: 'syncme-client conflicts list' prints no pending conflicts message when empty."""
        runner = CliRunner()
        with patch("client.cli.get_local_dbs") as mock_get_dbs:
            mock_db = MagicMock()
            mock_db.get_pending_conflicts.return_value = []
            mock_get_dbs.return_value = [mock_db]

            result = runner.invoke(cli, ["conflicts", "list"])
            assert result.exit_code == 0
            assert "no" in result.output.lower() or "0" in result.output or result.output.strip() == ""

    def test_cli_conflicts_list_with_records(self, tmp_path: Path):
        """Criterion: 'syncme-client conflicts list' displays pending conflicts with paths and IDs."""
        runner = CliRunner()
        with patch("client.cli.get_local_dbs") as mock_get_dbs:
            mock_db = MagicMock()
            mock_db.get_pending_conflicts.return_value = [
                {
                    "id": 1,
                    "workspace": "default",
                    "rel_path": "doc.txt",
                    "server_file": "/tmp/doc.txt",
                    "conflict_file": "/tmp/doc.conflict.1.txt",
                },
                {
                    "id": 2,
                    "workspace": "work",
                    "rel_path": "data.csv",
                    "server_file": "/tmp/data.csv",
                    "conflict_file": "/tmp/data.conflict.2.csv",
                },
            ]
            mock_get_dbs.return_value = [mock_db]

            result = runner.invoke(cli, ["conflicts", "list"])
            assert result.exit_code == 0
            assert "doc.txt" in result.output
            assert "data.csv" in result.output

    def test_cli_conflicts_count_plain(self):
        """Criterion: 'syncme-client conflicts count' outputs the numeric count of pending conflicts."""
        runner = CliRunner()
        with patch("client.cli.get_local_dbs") as mock_get_dbs:
            mock_db = MagicMock()
            mock_db.count_pending_conflicts.return_value = 3
            mock_get_dbs.return_value = [mock_db]

            result = runner.invoke(cli, ["conflicts", "count"])
            assert result.exit_code == 0
            assert "3" in result.output

    def test_cli_conflicts_count_waybar_format(self):
        """Criterion: 'syncme-client conflicts count --format waybar' outputs JSON with text, tooltip, and class."""
        runner = CliRunner()
        with patch("client.cli.get_local_dbs") as mock_get_dbs:
            mock_db = MagicMock()
            mock_db.count_pending_conflicts.return_value = 2
            mock_db.get_pending_conflicts.return_value = [
                {"id": 1, "rel_path": "a.txt", "workspace": "ws"},
                {"id": 2, "rel_path": "b.txt", "workspace": "ws"},
            ]
            mock_get_dbs.return_value = [mock_db]

            result = runner.invoke(cli, ["conflicts", "count", "--format", "waybar"])
            assert result.exit_code == 0
            data = json.loads(result.output.strip())
            assert "text" in data
            assert "tooltip" in data
            assert "class" in data
            assert "2" in str(data["text"])

    def test_cli_conflicts_count_waybar_format_zero(self):
        """Criterion: 'syncme-client conflicts count --format waybar' outputs clean state JSON when count is 0."""
        runner = CliRunner()
        with patch("client.cli.get_local_dbs") as mock_get_dbs:
            mock_db = MagicMock()
            mock_db.count_pending_conflicts.return_value = 0
            mock_db.get_pending_conflicts.return_value = []
            mock_get_dbs.return_value = [mock_db]

            result = runner.invoke(cli, ["conflicts", "count", "--format", "waybar"])
            assert result.exit_code == 0
            data = json.loads(result.output.strip())
            assert "text" in data
            assert "tooltip" in data
            assert "class" in data

    def test_cli_conflicts_resolve_interactive_local(self):
        """Criterion: 'syncme-client conflicts resolve' prompts for resolution; 'l' triggers local resolution."""
        runner = CliRunner()
        with patch("client.cli.get_local_dbs") as mock_get_dbs, \
             patch("client.cli.get_conflict_resolver") as mock_get_resolver, \
             patch("client.cli.format_inline_diff", return_value="mock diff"):
            mock_db = MagicMock()
            mock_db.get_pending_conflicts.return_value = [
                {"id": 42, "workspace": "default", "rel_path": "urgent.md", "server_file": "/tmp/s1", "conflict_file": "/tmp/c1"}
            ]
            mock_get_dbs.return_value = [mock_db]

            mock_resolver = MagicMock()
            mock_get_resolver.return_value = mock_resolver

            result = runner.invoke(cli, ["conflicts", "resolve"], input="l\n")
            assert result.exit_code == 0
            assert "mock diff" in result.output
            mock_resolver.resolve_conflict.assert_called_once()
            call_kwargs = mock_resolver.resolve_conflict.call_args[1] if mock_resolver.resolve_conflict.call_args[1] else {}
            call_args = mock_resolver.resolve_conflict.call_args[0]
            assert "local" in call_args or call_kwargs.get("action") == "local"

    def test_cli_conflicts_resolve_interactive_server(self):
        """Criterion: 'syncme-client conflicts resolve' prompts for resolution; 's' triggers server resolution."""
        runner = CliRunner()
        with patch("client.cli.get_local_dbs") as mock_get_dbs, \
             patch("client.cli.get_conflict_resolver") as mock_get_resolver, \
             patch("client.cli.format_inline_diff", return_value="mock diff"):
            mock_db = MagicMock()
            mock_db.get_pending_conflicts.return_value = [
                {"id": 10, "workspace": "default", "rel_path": "report.pdf", "server_file": "/tmp/s1", "conflict_file": "/tmp/c1"}
            ]
            mock_get_dbs.return_value = [mock_db]

            mock_resolver = MagicMock()
            mock_get_resolver.return_value = mock_resolver

            result = runner.invoke(cli, ["conflicts", "resolve"], input="s\n")
            assert result.exit_code == 0
            mock_resolver.resolve_conflict.assert_called_once()
            call_kwargs = mock_resolver.resolve_conflict.call_args[1] if mock_resolver.resolve_conflict.call_args[1] else {}
            call_args = mock_resolver.resolve_conflict.call_args[0]
            assert "server" in call_args or call_kwargs.get("action") == "server"

    def test_cli_conflicts_resolve_interactive_quit(self):
        """Criterion: 'syncme-client conflicts resolve' aborts prompt cleanly when user chooses 'q'."""
        runner = CliRunner()
        with patch("client.cli.get_local_dbs") as mock_get_dbs, \
             patch("client.cli.get_conflict_resolver") as mock_get_resolver, \
             patch("client.cli.format_inline_diff", return_value="mock diff"):
            mock_db = MagicMock()
            mock_db.get_pending_conflicts.return_value = [
                {"id": 1, "workspace": "default", "rel_path": "a.txt", "server_file": "/tmp/s1", "conflict_file": "/tmp/c1"},
                {"id": 2, "workspace": "default", "rel_path": "b.txt", "server_file": "/tmp/s2", "conflict_file": "/tmp/c2"},
            ]
            mock_get_dbs.return_value = [mock_db]

            mock_resolver = MagicMock()
            mock_get_resolver.return_value = mock_resolver

            result = runner.invoke(cli, ["conflicts", "resolve"], input="q\n")
            assert result.exit_code == 0
            mock_resolver.resolve_conflict.assert_not_called()

    def test_cli_conflicts_resolve_multiple_sequence(self):
        """Criterion: Resolving multiple conflicts in the same workspace in sequence works without error (API client stays open)."""
        runner = CliRunner()
        with patch("client.cli.get_local_dbs") as mock_get_dbs, \
             patch("client.cli.get_conflict_resolver") as mock_get_resolver, \
             patch("client.cli.format_inline_diff", return_value="mock diff"):
            mock_db = MagicMock()
            mock_db.get_pending_conflicts.return_value = [
                {"id": 1, "workspace": "default", "rel_path": "a.txt", "server_file": "/tmp/s1", "conflict_file": "/tmp/c1"},
                {"id": 2, "workspace": "default", "rel_path": "b.txt", "server_file": "/tmp/s2", "conflict_file": "/tmp/c2"},
            ]
            mock_get_dbs.return_value = [("default", mock_db)]

            mock_resolver = MagicMock()
            mock_resolver.resolve_conflict = AsyncMock(return_value=True)
            mock_resolver.api = MagicMock()
            mock_resolver.api.close = AsyncMock()
            mock_get_resolver.return_value = mock_resolver

            result = runner.invoke(cli, ["conflicts", "resolve"], input="l\ns\n")
            assert result.exit_code == 0
            assert mock_resolver.resolve_conflict.call_count == 2
            mock_resolver.api.close.assert_called_once()

# ===========================================================================
# 4. format_inline_diff Tests (client/conflicts.py)
# ===========================================================================


def test_format_inline_diff_identical(tmp_path):
    s = tmp_path / "server.txt"
    c = tmp_path / "conflict.txt"
    s.write_text("hello\nworld\n")
    c.write_text("hello\nworld\n")
    diff = format_inline_diff(s, c)
    assert "identical" in diff

def test_format_inline_diff_binary(tmp_path):
    s = tmp_path / "server.bin"
    c = tmp_path / "conflict.bin"
    s.write_bytes(b"hello\x00world")
    c.write_bytes(b"hello\x00there")
    diff = format_inline_diff(s, c)
    assert "Binary files differ" in diff

def test_format_inline_diff_text(tmp_path):
    s = tmp_path / "server.txt"
    c = tmp_path / "conflict.txt"
    s.write_text("hello\nworld\n")
    c.write_text("hello\nthere\n")
    diff = format_inline_diff(s, c)
    assert "--- server" in diff
    assert "+++ local (.conflict)" in diff
    assert "-world" in diff
    assert "+there" in diff

def test_format_inline_diff_missing_file(tmp_path):
    s = tmp_path / "missing_server.txt"
    c = tmp_path / "missing_conflict.txt"
    diff = format_inline_diff(s, c)
    assert "(File missing on disk)" in diff

def test_format_inline_diff_large_file(tmp_path):
    s = tmp_path / "server.txt"
    c = tmp_path / "conflict.txt"
    s.write_text("a" * 1024)
    c.write_text("b" * 1024)
    diff = format_inline_diff(s, c, max_bytes=500)
    assert "too large" in diff

def test_format_inline_diff_ansi_sanitization(tmp_path):
    s = tmp_path / "server.txt"
    c = tmp_path / "conflict.txt"
    s.write_text("hello\x1b[31mworld\n")
    c.write_text("hello\x07there\n")
    diff = format_inline_diff(s, c)
    assert "hello?[31mworld" in diff
    assert "hello?there" in diff

def test_format_inline_diff_truncation(tmp_path):
    s = tmp_path / "server.txt"
    c = tmp_path / "conflict.txt"
    s.write_text("\n".join(str(i) for i in range(100)))
    c.write_text("\n".join(str(i) for i in range(100, 200)))
    diff = format_inline_diff(s, c, max_lines=10)
    assert "(truncated remaining diff lines)" in diff
