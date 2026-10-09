import hashlib
import os
from pathlib import Path


class UnsafePathError(ValueError):
    pass


def safe_join(root: Path, rel: str) -> Path:
    """Join a server-supplied relative path onto root, or raise UnsafePathError.

    Only canonical paths are accepted (no empty, "." or ".." parts, no leading
    or doubled "/"), and the real location of the parent directory must stay
    inside the real location of root, which also rejects symlinked parents.
    """
    if not isinstance(rel, str) or not rel or "\x00" in rel or "\\" in rel or rel.startswith("/"):
        raise UnsafePathError(f"unsafe path: {rel!r}")
    parts = rel.split("/")
    if any(p in ("", ".", "..") for p in parts):
        raise UnsafePathError(f"unsafe path: {rel!r}")
    full = Path(root).joinpath(*parts)
    real_root = Path(os.path.realpath(root))
    real_parent = Path(os.path.realpath(full.parent))
    if not real_parent.is_relative_to(real_root):
        raise UnsafePathError(f"path escapes sync root: {rel!r}")
    return full


def sha256_matches(content: bytes, expected: str | None) -> bool:
    if not expected:
        return True
    return hashlib.sha256(content).hexdigest() == expected
