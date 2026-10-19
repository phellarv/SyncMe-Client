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

def test_archive_restore_cli_with_underscores(tmp_path: Path):
    from unittest.mock import patch, MagicMock
    from click.testing import CliRunner
    from client.cli import cli

    test_file = tmp_path / "my_cool_script.py"
    test_file.write_text("print('hello')")
    archive_name = archive_file(tmp_path, "my_cool_script.py")
    test_file.unlink()

    d_conf = MagicMock()
    d_conf.local_path = tmp_path
    d_conf.name = "default"
    engine = MagicMock()

    with patch("client.cli._get_engines", return_value=[(d_conf, engine)]):
        runner = CliRunner()
        result = runner.invoke(cli, ["archive", "restore", archive_name])
        assert result.exit_code == 0
        restored = tmp_path / "my_cool_script.py"
        assert restored.exists(), f"Expected my_cool_script.py to exist, but found {[p.name for p in tmp_path.iterdir()]}"
        assert restored.read_text() == "print('hello')"


def test_restore_file_rejects_archive_name_traversal(tmp_path: Path):
    import pytest
    from client.pathsafe import UnsafePathError

    secret = tmp_path / ".syncme" / "secret.txt"
    secret.parent.mkdir(parents=True, exist_ok=True)
    secret.write_text("top secret")

    with pytest.raises(UnsafePathError):
        restore_file(tmp_path, "../secret.txt", "leaked.txt")
    assert not (tmp_path / "leaked.txt").exists()


def test_archive_file_same_second_collision_adds_suffix(tmp_path: Path, monkeypatch):
    monkeypatch.setattr("client.archive.time.time", lambda: 1700000000)
    f = tmp_path / "notes.txt"
    f.write_text("v1")
    name1 = archive_file(tmp_path, "notes.txt")
    f.write_text("v2")
    name2 = archive_file(tmp_path, "notes.txt")

    assert name1 == "notes.txt_1700000000"
    assert name2 == "notes.txt_1700000000-1"
    archive_dir = tmp_path / ".syncme" / "archive"
    assert (archive_dir / name1).read_text() == "v1"
    assert (archive_dir / name2).read_text() == "v2"


def test_archive_cli_standalone_files(tmp_path: Path):
    from unittest.mock import patch, MagicMock
    from click.testing import CliRunner
    from client.cli import cli
    from client.config import FileConfig

    state_dir = tmp_path / "state"
    state_dir.mkdir()
    conf_dir = tmp_path / "config_dir"
    conf_dir.mkdir()
    standalone = conf_dir / "rc.conf"
    standalone.write_text("archived standalone")

    archive_name = archive_file(conf_dir, "rc.conf")
    standalone.unlink()

    d_conf = MagicMock()
    d_conf.local_path = state_dir
    d_conf.name = "files"
    engine = MagicMock()
    engine.files = [FileConfig(local_path=standalone, workspace="files", remote_path="rc.conf")]

    with patch("client.cli._get_engines", return_value=[(d_conf, engine)]):
        runner = CliRunner()
        res_list = runner.invoke(cli, ["archive", "list"])
        assert res_list.exit_code == 0
        assert archive_name in res_list.output

        res_restore = runner.invoke(cli, ["archive", "restore", archive_name])
        assert res_restore.exit_code == 0
        assert standalone.exists()
        assert standalone.read_text() == "archived standalone"
