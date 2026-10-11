from pathlib import Path
from client.ignore import is_ignored, IgnoreParser

def test_ignore_defaults(tmp_path: Path):
    assert is_ignored(tmp_path, ".smignore") is True
    assert is_ignored(tmp_path, ".syncme/local_state.db") is True
    assert is_ignored(tmp_path, ".git/config") is True
    assert is_ignored(tmp_path, "file.conflict.123.txt") is True
    assert is_ignored(tmp_path, "normal_file.txt") is False

def test_ignore_custom(tmp_path: Path):
    ignore_file = tmp_path / ".smignore"
    ignore_file.write_text("*.log\n// comment\n# another comment\n(?i)test.TXT\n!important.log\n")
    
    assert is_ignored(tmp_path, "app.log") is True
    assert is_ignored(tmp_path, "important.log") is False
    assert is_ignored(tmp_path, "test.TXT") is True

def test_ignore_advanced(tmp_path: Path):
    ignore_file = tmp_path / ".smignore"
    ignore_file.write_text(
        "plugins/cache/\n"
        "foo/bar # comment\n"
        "(?d)(?i)somedir/\n"
        "(?i)plugins/cache/\n"
    )
    
    assert is_ignored(tmp_path, "plugins/cache/file.txt") is True
    assert is_ignored(tmp_path, "plugins/cache/sub/nested.txt") is True
    assert is_ignored(tmp_path, "sub/plugins/cache/file.txt") is True
    assert is_ignored(tmp_path, "plugins/cache") is True
    
    assert is_ignored(tmp_path, "foo/bar") is True
    assert is_ignored(tmp_path, "foo/bar/baz") is True
    
    assert is_ignored(tmp_path, "somedir/file.txt") is True
    assert is_ignored(tmp_path, "Somedir/file.txt") is True
    
    assert is_ignored(tmp_path, "Plugins/Cache/file.txt") is True

def test_ignore_extra_patterns(tmp_path: Path):
    assert is_ignored(tmp_path, "fil1", extra_patterns=["fil1", "fil2"]) is True
    assert is_ignored(tmp_path, "fil2", extra_patterns=["fil1", "fil2"]) is True
    assert is_ignored(tmp_path, "fil3", extra_patterns=["fil1", "fil2"]) is False
    assert is_ignored(tmp_path, "sub/fil1", extra_patterns=["fil1", "fil2"]) is True
    # Filenames containing # or starting with # are not treated as comments in extra_patterns
    assert is_ignored(tmp_path, "rapport #1.txt", extra_patterns=["rapport #1.txt"]) is True
    assert is_ignored(tmp_path, "#notes", extra_patterns=["#notes"]) is True
    # Default ignores remain ignored regardless
    assert is_ignored(tmp_path, ".git/config", extra_patterns=["fil1"]) is True
    assert is_ignored(tmp_path, ".syncme/local_state.db", extra_patterns=["fil1"]) is True
