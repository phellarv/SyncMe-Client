import difflib
import click
import asyncio
import inspect
import shutil
import os
import hashlib
from pathlib import Path

class ConflictResolver:
    def __init__(self, db, api=None, runner=None):
        self.db = db
        self.api = api
        self.runner = runner

    def _compute_sha256(self, path: str) -> str:
        h = hashlib.sha256()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                h.update(chunk)
        return h.hexdigest()

    async def resolve_conflict(self, conflict_id: int, action: str) -> bool:
        conflict = self.db.get_conflict(conflict_id)
        if not conflict:
            return False

        server_file = conflict['server_file']
        conflict_file = conflict['conflict_file']
        rel_path = conflict['rel_path']

        if action == 'local':
            if not os.path.exists(conflict_file):
                return False
            shutil.copy2(conflict_file, server_file)
            if self.api:
                if inspect.iscoroutinefunction(self.api.upload_file):
                    res = await self.api.upload_file(rel_path, server_file)
                else:
                    res = self.api.upload_file(rel_path, server_file)
                
                if res and 'revision' in res:
                    rev = res['revision']
                    sha256 = self._compute_sha256(server_file)
                    st = os.stat(server_file)
                    self.db.upsert_file(rel_path, sha256, st.st_size, st.st_mtime, rev, False)
                    try:
                        os.unlink(conflict_file)
                    except OSError:
                        pass
                    self.db.resolve_conflict_record(conflict_id, 'local')
                    return True
                else:
                    return False
            else:
                try:
                    os.unlink(conflict_file)
                except OSError:
                    pass
                self.db.resolve_conflict_record(conflict_id, 'local')
                return True

        elif action == 'server':
            try:
                os.unlink(conflict_file)
            except OSError:
                pass
            self.db.resolve_conflict_record(conflict_id, 'server')
            return True

        elif action == 'meld':
            meld_cmd = ['meld', server_file, conflict_file]
            if self.runner:
                res = self.runner(meld_cmd)
                if asyncio.iscoroutine(res):
                    returncode = await res
                else:
                    returncode = res
            else:
                proc = await asyncio.create_subprocess_exec(*meld_cmd)
                returncode = await proc.wait()

            if returncode == 0:
                if self.api:
                    if inspect.iscoroutinefunction(self.api.upload_file):
                        res = await self.api.upload_file(rel_path, server_file)
                    else:
                        res = self.api.upload_file(rel_path, server_file)
                        
                    if res and 'revision' in res:
                        rev = res['revision']
                        sha256 = self._compute_sha256(server_file)
                        st = os.stat(server_file)
                        self.db.upsert_file(rel_path, sha256, st.st_size, st.st_mtime, rev, False)
                    else:
                        return False
                        
                try:
                    os.unlink(conflict_file)
                except OSError:
                    pass
                self.db.resolve_conflict_record(conflict_id, 'meld')
                return True
            else:
                return False

        elif action == 'dismiss':
            self.db.dismiss_conflict(conflict_id)
            return True

        return False

def format_inline_diff(server_file: Path | str, conflict_file: Path | str, max_lines: int = 60, max_bytes: int = 1_048_576) -> str:
    s_path = Path(server_file)
    c_path = Path(conflict_file)
    
    if not s_path.is_file() or not c_path.is_file():
        return click.style("(File missing on disk)", fg="red")
        
    try:
        if s_path.stat().st_size > max_bytes or c_path.stat().st_size > max_bytes:
            return click.style(f"(File too large for inline diff > 1MB)", fg="yellow")
    except Exception:
        return click.style("(File missing on disk)", fg="red")
        
    def is_binary(path: Path) -> bool:
        try:
            with open(path, 'rb') as f:
                chunk = f.read(1024)
                if b'\0' in chunk:
                    return True
        except Exception:
            return True
        return False

    if is_binary(s_path) or is_binary(c_path):
        return click.style("(Binary files differ - text diff unavailable)", fg="yellow")

    try:
        with open(s_path, 'r', encoding='utf-8', errors='replace') as f:
            server_lines = f.read().splitlines()
        with open(c_path, 'r', encoding='utf-8', errors='replace') as f:
            conflict_lines = f.read().splitlines()
    except Exception:
        return click.style("(Error reading files for diff)", fg="red")

    def sanitize(lines):
        out = []
        for line in lines:
            sanitized = []
            for ch in line:
                if (ch < ' ' and ch not in ('\t', '\n')) or ch == '\x1b':
                    sanitized.append('?')
                else:
                    sanitized.append(ch)
            out.append(''.join(sanitized))
        return out
        
    server_lines = sanitize(server_lines)
    conflict_lines = sanitize(conflict_lines)

    diff = list(difflib.unified_diff(
        server_lines, conflict_lines,
        fromfile="server", tofile="local (.conflict)",
        lineterm=""
    ))

    if not diff:
        return click.style("(Files are identical in text content)", fg="yellow")

    colored_diff = []
    for i, line in enumerate(diff):
        if i >= max_lines:
            colored_diff.append(click.style("... (truncated remaining diff lines)", fg="yellow"))
            break
        if line.startswith('---') or line.startswith('+++'):
            colored_diff.append(click.style(line, bold=True))
        elif line.startswith('@@'):
            colored_diff.append(click.style(line, fg="cyan"))
        elif line.startswith('-'):
            colored_diff.append(click.style(line, fg="red"))
        elif line.startswith('+'):
            colored_diff.append(click.style(line, fg="green"))
        else:
            colored_diff.append(line)

    return "\n".join(colored_diff)
