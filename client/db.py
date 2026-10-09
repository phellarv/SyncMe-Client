import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, List

class LocalDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA busy_timeout=5000;")
        self._init_db()

    def _init_db(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    path TEXT PRIMARY KEY,
                    sha256 TEXT,
                    size INTEGER,
                    mtime REAL,
                    server_revision INTEGER,
                    is_deleted BOOLEAN
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS conflicts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace TEXT NOT NULL,
                    rel_path TEXT NOT NULL,
                    server_file TEXT NOT NULL,
                    conflict_file TEXT NOT NULL,
                    detected_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    resolved BOOLEAN NOT NULL DEFAULT 0,
                    resolution TEXT,
                    resolved_at REAL
                )
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_conflicts_status ON conflicts(status, workspace)
            """)
            self.conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_conflicts_pending_uniq ON conflicts(workspace, rel_path) WHERE status = 'pending'
            """)

    def get_file(self, path: str) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM files WHERE path = ?", (path,))
        row = cur.fetchone()
        if row:
            return dict(row)
        return None

    def upsert_file(self, path: str, sha256: str, size: int, mtime: float, server_revision: int, is_deleted: bool):
        with self.conn:
            self.conn.execute("""
                INSERT INTO files (path, sha256, size, mtime, server_revision, is_deleted)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    sha256=excluded.sha256,
                    size=excluded.size,
                    mtime=excluded.mtime,
                    server_revision=excluded.server_revision,
                    is_deleted=excluded.is_deleted
            """, (path, sha256, size, mtime, server_revision, is_deleted))

    def delete_file(self, path: str):
        with self.conn:
            self.conn.execute("DELETE FROM files WHERE path = ?", (path,))

    def get_all_files(self) -> List[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM files")
        return [dict(r) for r in cur.fetchall()]

    def get_last_server_revision(self) -> int:
        cur = self.conn.execute("SELECT value FROM metadata WHERE key = 'last_server_revision'")
        row = cur.fetchone()
        if row:
            return int(row["value"])
        return 0

    def set_last_server_revision(self, revision: int):
        with self.conn:
            self.conn.execute("""
                INSERT INTO metadata (key, value)
                VALUES ('last_server_revision', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, (str(revision),))

    def is_initialized(self) -> bool:
        cur = self.conn.execute("SELECT value FROM metadata WHERE key = 'initialized'")
        row = cur.fetchone()
        if row:
            return row["value"] in ("1", "true", "True")
        return False

    def set_initialized(self, val: bool = True):
        with self.conn:
            self.conn.execute("""
                INSERT INTO metadata (key, value)
                VALUES ('initialized', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """, ("1" if val else "0",))


    def record_conflict(self, workspace: str, rel_path: str, server_file: str, conflict_file: str) -> int:
        import time
        with self.conn:
            cur = self.conn.execute("SELECT id, conflict_file FROM conflicts WHERE workspace = ? AND rel_path = ? AND status = 'pending'", (workspace, rel_path))
            row = cur.fetchone()
            if row:
                conflict_id = row['id']
                old_conflict_file = row['conflict_file']
                if old_conflict_file and old_conflict_file != conflict_file:
                    try:
                        Path(old_conflict_file).unlink(missing_ok=True)
                    except Exception:
                        pass
                self.conn.execute("UPDATE conflicts SET server_file = ?, conflict_file = ?, detected_at = ? WHERE id = ?", (server_file, conflict_file, time.time(), conflict_id))
                return conflict_id
            else:
                cur = self.conn.execute(
                    "INSERT INTO conflicts (workspace, rel_path, server_file, conflict_file, detected_at) VALUES (?, ?, ?, ?, ?)",
                    (workspace, rel_path, server_file, conflict_file, time.time())
                )
                return cur.lastrowid

    def get_conflict(self, conflict_id: int) -> Optional[Dict[str, Any]]:
        cur = self.conn.execute("SELECT * FROM conflicts WHERE id = ?", (conflict_id,))
        row = cur.fetchone()
        if row:
            res = dict(row)
            res['resolved'] = bool(res['resolved'])
            return res
        return None

    def get_pending_conflicts(self, workspace: str = None) -> List[Dict[str, Any]]:
        if workspace:
            cur = self.conn.execute("SELECT * FROM conflicts WHERE status = 'pending' AND workspace = ? ORDER BY detected_at DESC", (workspace,))
        else:
            cur = self.conn.execute("SELECT * FROM conflicts WHERE status = 'pending' ORDER BY detected_at DESC")
        
        records = [dict(r) for r in cur.fetchall()]
        active_records = []
        for record in records:
            if not Path(record['conflict_file']).exists():
                self.dismiss_conflict(record['id'])
            else:
                active_records.append(record)
        return active_records

    def count_pending_conflicts(self, workspace: str = None) -> int:
        return len(self.get_pending_conflicts(workspace))

    def resolve_conflict_record(self, conflict_id: int, resolution: str):
        import time
        with self.conn:
            self.conn.execute("UPDATE conflicts SET status = 'resolved', resolved = 1, resolution = ?, resolved_at = ? WHERE id = ? AND status = 'pending'", (resolution, time.time(), conflict_id))

    def dismiss_conflict(self, conflict_id: int):
        import time
        with self.conn:
            self.conn.execute("UPDATE conflicts SET status = 'dismissed', resolved = 1, resolution = 'dismissed', resolved_at = ? WHERE id = ? AND status = 'pending'", (time.time(), conflict_id))

    def close(self):
        self.conn.close()
