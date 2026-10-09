import hashlib

import pytest

from client.pathsafe import UnsafePathError, safe_join, sha256_matches


def test_safe_join_ok(tmp_path):
    assert safe_join(tmp_path, "a/b.txt") == tmp_path / "a" / "b.txt"


@pytest.mark.parametrize(
    "rel",
    ["", "/etc/passwd", "..", "../x", "a/../b", "a\\b", "a\x00b", "a//b", "a/./b", "./a", "a/"],
)
def test_safe_join_rejects(tmp_path, rel):
    with pytest.raises(UnsafePathError):
        safe_join(tmp_path, rel)


@pytest.mark.parametrize("rel", [42, b"a/b", b"", None, 1.5, ["a", "b"]])
def test_safe_join_rejects_non_str(tmp_path, rel):
    with pytest.raises(UnsafePathError):
        safe_join(tmp_path, rel)


def test_safe_join_rejects_symlinked_parent(tmp_path):
    root = tmp_path / "sync"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "link").symlink_to(outside)
    with pytest.raises(UnsafePathError):
        safe_join(root, "link/pwn.txt")


def test_safe_join_allows_symlink_inside_root(tmp_path):
    root = tmp_path / "sync"
    (root / "real").mkdir(parents=True)
    (root / "alias").symlink_to(root / "real")
    assert safe_join(root, "alias/x.txt") == root / "alias" / "x.txt"


def test_sha256_matches():
    data = b"hello"
    assert sha256_matches(data, hashlib.sha256(data).hexdigest())
    assert not sha256_matches(data, "0" * 64)
    assert sha256_matches(data, None)
    assert sha256_matches(data, "")
