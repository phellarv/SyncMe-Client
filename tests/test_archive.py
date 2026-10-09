from pathlib import Path
from client.archive import archive_file, list_archive, restore_file

def test_archive_restore(tmp_path: Path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello")
    
    archive_name = archive_file(tmp_path, "test.txt")
    assert archive_name is not None
    assert archive_name.startswith("test.txt_")
    
    archives = list_archive(tmp_path)
    assert len(archives) == 1
    assert archives[0]['name'] == archive_name
    
    restore_file(tmp_path, archive_name, "restored.txt")
    restored = tmp_path / "restored.txt"
    assert restored.exists()
    assert restored.read_text() == "hello"
