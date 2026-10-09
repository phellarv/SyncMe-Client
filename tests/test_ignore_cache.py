"""is_ignored() is called for every directory and file of every reconcile pass.

It used to build a new IgnoreParser on each call: read .smignore and compile every pattern again.
That was about 90% of the scan time and kept the daemon near one busy core. The parser is now
built once per sync directory and rebuilt only when .smignore changes.
"""
import os
from pathlib import Path

from client import ignore
from client.ignore import is_ignored


def count_compiles(monkeypatch):
    calls = []
    original = ignore.PathSpec.from_lines

    def counting(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(ignore.PathSpec, "from_lines", counting)
    return calls


def test_parser_is_built_once_per_sync_dir(tmp_path: Path, monkeypatch):
    (tmp_path / ".smignore").write_text("*.log\n")
    is_ignored(tmp_path, "warm-up.txt")  # a first call may build the parser
    calls = count_compiles(monkeypatch)

    for i in range(200):
        is_ignored(tmp_path, f"dir/file{i}.txt")
        is_ignored(tmp_path, f"dir/file{i}.log")

    assert calls == []


def test_edited_smignore_takes_effect_at_once(tmp_path: Path):
    smignore = tmp_path / ".smignore"
    smignore.write_text("*.log\n")
    assert is_ignored(tmp_path, "a.log") is True
    assert is_ignored(tmp_path, "a.tmp") is False

    smignore.write_text("*.tmp\n")
    st = smignore.stat()
    os.utime(smignore, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000_000))  # not within one clock tick

    assert is_ignored(tmp_path, "a.log") is False
    assert is_ignored(tmp_path, "a.tmp") is True


def test_smignore_created_and_removed_later(tmp_path: Path):
    assert is_ignored(tmp_path, "a.log") is False

    (tmp_path / ".smignore").write_text("*.log\n")
    assert is_ignored(tmp_path, "a.log") is True

    (tmp_path / ".smignore").unlink()
    assert is_ignored(tmp_path, "a.log") is False


def test_sync_dirs_do_not_share_rules(tmp_path: Path):
    one, two = tmp_path / "one", tmp_path / "two"
    one.mkdir()
    two.mkdir()
    (one / ".smignore").write_text("*.log\n")
    (two / ".smignore").write_text("*.tmp\n")

    assert is_ignored(one, "a.log") is True
    assert is_ignored(two, "a.log") is False
    assert is_ignored(one, "a.tmp") is False
    assert is_ignored(two, "a.tmp") is True
