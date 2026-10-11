import re
import os
from pathlib import Path
from typing import Sequence
from pathspec import PathSpec


DEFAULT_IGNORES = [
    ".smignore",
    ".syncme/",
    ".git/",
    "*.conflict.*",
    ".syncme-tmp-*",
    ".syncme-upload-*"
]
DEFAULT_SPEC = PathSpec.from_lines("gitignore", DEFAULT_IGNORES)

class IgnoreParser:
    def __init__(self, sync_dir: Path, extra_patterns: Sequence[str] = ()):
        self.sync_dir = sync_dir
        self.extra_patterns = tuple(extra_patterns or ())
        self.patterns = []
        self.spec = None
        self.case_spec = None
        self.load()

    def _process_line(self, line: str, standard_lines: list, case_lines: list, strip_comments: bool = True):
        line = line.strip()
        if not line:
            return
        if strip_comments:
            if line.startswith("//") or line.startswith("#"):
                return
            clean_line = line.split(" #")[0].split(" //")[0].strip()
            if not clean_line or clean_line.startswith("//") or clean_line.startswith("#"):
                return
        else:
            clean_line = line

        case_insensitive = False
        while True:
            if clean_line.startswith("(?d)"):
                clean_line = clean_line[4:]
            elif clean_line.startswith("(?i)"):
                clean_line = clean_line[4:]
                case_insensitive = True
            else:
                break
                
        if clean_line:
            if case_insensitive:
                clean_line = clean_line.lower()
            lines_list = case_lines if case_insensitive else standard_lines
            if not clean_line.startswith("/") and not clean_line.startswith("**/"):
                lines_list.append(clean_line)
                lines_list.append(f"**/{clean_line.lstrip('/')}")
            else:
                lines_list.append(clean_line)

    def load(self):
        standard_lines = list(DEFAULT_IGNORES)
        case_lines = []
        ignore_file = self.sync_dir / ".smignore"
        if ignore_file.is_file():
            try:
                with open(ignore_file, "r", encoding="utf-8") as f:
                    for line in f:
                        self._process_line(line, standard_lines, case_lines, strip_comments=True)
            except Exception:
                pass
        
        for pat in self.extra_patterns:
            self._process_line(pat, standard_lines, case_lines, strip_comments=False)

        self.spec = PathSpec.from_lines("gitignore", standard_lines)
        if case_lines:
            self.case_spec = PathSpec.from_lines("gitignore", case_lines)

    def is_ignored(self, rel_path: str) -> bool:
        # Normalize path to use forward slashes
        rel_path = rel_path.replace(os.sep, "/")

        if DEFAULT_SPEC.match_file(rel_path) or DEFAULT_SPEC.match_file(rel_path + "/"):
            return True

        if not self.spec:
            return False
        
        if self.spec.match_file(rel_path) or self.spec.match_file(rel_path + "/"):
            return True
            
        if self.case_spec:
            if self.case_spec.match_file(rel_path.lower()) or self.case_spec.match_file(rel_path.lower() + "/"):
                return True
                
        return False

# One parser per (sync directory, extra_patterns), rebuilt only when .smignore changes. Compiling the patterns on every
# call made a reconcile pass of a few thousand files cost seconds of CPU.
_parsers: dict[tuple[str, tuple], tuple[tuple | None, IgnoreParser]] = {}

def _smignore_signature(sync_dir: Path) -> tuple | None:
    try:
        st = (sync_dir / ".smignore").stat()
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size, st.st_ino)

def is_ignored(sync_dir: Path, rel_path: str, extra_patterns: Sequence[str] = ()) -> bool:
    # The signature is read before the parser is built: a change in between only costs one extra rebuild.
    signature = _smignore_signature(sync_dir)
    cache_key = (str(sync_dir), tuple(extra_patterns or ()))
    cached = _parsers.get(cache_key)
    if cached is None or cached[0] != signature:
        cached = (signature, IgnoreParser(sync_dir, extra_patterns=extra_patterns))
        _parsers[cache_key] = cached
    return cached[1].is_ignored(rel_path)
