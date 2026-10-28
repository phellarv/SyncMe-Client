import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from click.testing import CliRunner
from client.cli import cli


def _engine_with_conflicts(paths, seen):
    engine = MagicMock()
    engine.db.is_initialized.return_value = False
    engine.api.close = AsyncMock()

    async def initial_sync(cb):
        for p in paths:
            seen.append((p, cb(p, {"size": 1, "mtime": 1}, {"size": 2, "mtime": 2})))
        return {"uploaded": 0, "downloaded": 0, "conflicts": len(paths), "unchanged": 0}

    engine.initial_sync = initial_sync
    return engine


def _conf(name):
    d_conf = MagicMock()
    d_conf.name = name
    d_conf.conflict_strategy = "ask"
    return d_conf


@pytest.fixture
def run_init():
    def _run(engines, user_input):
        with patch("client.cli._get_engines", return_value=engines), \
             patch("client.cli._require_server_config"), \
             patch("client.cli.save_default_config"):
            return CliRunner().invoke(cli, ["init"], input=user_input)
    return _run


@pytest.mark.parametrize("key,expected", [("S", "s"), ("L", "l")])
def test_apply_to_all_stops_asking(run_init, key, expected):
    seen = []
    engine = _engine_with_conflicts(["a", "b", "c"], seen)
    result = run_init([(_conf("default"), engine)], f"b\n{key}\n")
    assert result.exit_code == 0, result.output
    assert seen == [("a", "b"), ("b", expected), ("c", expected)]
    # Only two prompts were shown: the third conflict was resolved silently.
    assert result.output.count("Choose action") == 2


def test_apply_to_all_carries_over_to_next_directory(run_init):
    seen = []
    engines = [(_conf("one"), _engine_with_conflicts(["a"], seen)),
               (_conf("two"), _engine_with_conflicts(["x", "y"], seen))]
    result = run_init(engines, "L\n")
    assert result.exit_code == 0, result.output
    assert seen == [("a", "l"), ("x", "l"), ("y", "l")]
    assert result.output.count("Choose action") == 1


def test_lowercase_choices_still_ask_every_time(run_init):
    seen = []
    engine = _engine_with_conflicts(["a", "b"], seen)
    result = run_init([(_conf("default"), engine)], "s\nl\n")
    assert result.exit_code == 0, result.output
    assert seen == [("a", "s"), ("b", "l")]
    assert result.output.count("Choose action") == 2
