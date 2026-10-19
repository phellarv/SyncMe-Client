from pathlib import Path
from client.db import LocalDB

def test_db_operations(tmp_path: Path):
    db = LocalDB(tmp_path / ".syncme" / "local_state.db")
    
    # Test last server revision
    assert db.get_last_server_revision() == 0
    db.set_last_server_revision(5)
    assert db.get_last_server_revision() == 5
    
    # Test file operations
    db.upsert_file("test.txt", "hash123", 100, 1234.0, 1, False)
    
    f = db.get_file("test.txt")
    assert f is not None
    assert f['path'] == "test.txt"
    assert f['sha256'] == "hash123"
    assert f['size'] == 100
    assert f['server_revision'] == 1
    
    files = db.get_all_files()
    assert len(files) == 1
    
    db.delete_file("test.txt")
    assert db.get_file("test.txt") is None
    
    db.close()


def test_localdb_context_manager(tmp_path: Path):
    db_path = tmp_path / "test_cm.db"
    with LocalDB(db_path) as db:
        assert not db.is_initialized()
        db.set_initialized(True)
        assert db.is_initialized()
        assert db.get_last_server_revision() == 0
    # After exiting, connection should be closed
    import sqlite3
    import pytest
    with pytest.raises(sqlite3.ProgrammingError):
        db.conn.execute("SELECT 1")
