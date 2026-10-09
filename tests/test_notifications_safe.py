"""_safe() strips control and format characters from server-supplied notification text.

Notification text comes from the server (paths). Escape and OSC sequences, bidi overrides,
zero-width characters and newlines must not reach the desktop's notification daemon: each run of
Unicode categories Cc, Cf, Zl and Zp becomes one space, then the text is truncated, then &, < and
> are escaped.
"""
import pytest

from client.notifications import _safe, handle_conflict_notification


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("a\x1b[31mred\x1b[0m", "a [31mred [0m"),                    # ANSI colour (ESC is Cc)
        ("x\x1b]0;evil title\x07y", "x ]0;evil title y"),            # OSC set-title ... BEL
        ("doc‮txt.exe", "doc txt.exe"),                         # right-to-left override (Cf)
        ("a​b", "a b"),                                         # zero-width space (Cf)
        ("a‍b", "a b"),                                         # zero-width joiner (Cf)
        ("a\nb", "a b"),
        ("a\rb", "a b"),
        ("a\x00b", "a b"),
        ("a\tb", "a b"),                                             # tab is Cc too
        ("a b", "a b"),                                         # line separator (Zl)
        ("a b", "a b"),                                         # paragraph separator (Zp)
        ("a\x7fb", "a b"),                                           # DEL (Cc)
        ("a\x85b", "a b"),                                           # NEL (Cc)
    ],
)
def test_each_control_or_format_character_becomes_one_space(raw, expected):
    assert _safe(raw) == expected


def test_a_run_of_replaced_characters_collapses_to_a_single_space():
    assert _safe("a\r\nb") == "a b"
    assert _safe("a\x00\x01\x02​‪ b") == "a b"
    assert _safe("\x1b\x1b\x1b") == " "


def test_the_users_own_spaces_are_left_alone():
    assert _safe("a  b") == "a  b"
    assert _safe("a \n b") == "a   b"  # space, one replacement space, space


@pytest.mark.parametrize("text", ["plain.txt", "dir/sub/näme ünï.txt", "日本語/ファイル.txt", "🔍 notes ☁️.md", "100% (1).txt"])
def test_ordinary_text_passes_through_unchanged(text):
    assert _safe(text) == text


def test_replacement_comes_before_truncation():
    """Five NULs collapse to one space, so 1 + 118 characters fit the limit: no ellipsis."""
    assert _safe("\x00" * 5 + "x" * 118) == " " + "x" * 118


def test_truncation_still_applies_after_the_replacement():
    assert _safe("\x00\x00" + "x" * 200) == " " + "x" * 119 + "…"


def test_truncation_comes_before_escaping():
    assert _safe("<" * 130) == "&lt;" * 120 + "…"


def test_markup_is_escaped_and_quotes_are_left_alone():
    assert _safe('<b>"x" & \'y\'</b>') == "&lt;b&gt;\"x\" &amp; 'y'&lt;/b&gt;"


@pytest.mark.asyncio
async def test_the_notify_send_arguments_carry_no_control_characters(tmp_path):
    cmds = []

    def runner(cmd):
        cmds.append(list(cmd))
        return (0, "")

    evil = "a\x1b[31m\x1b]0;t\x07‮​\n\r\x00b.txt"
    await handle_conflict_notification(
        workspace="w\nx\x1b", rel_path=evil, server_file=tmp_path / evil.replace("\n", "").replace("\r", "").replace("\x00", ""),
        conflict_file=tmp_path / "c.txt", runner=runner,
    )
    title, body = cmds[0][7], cmds[0][8]
    for text in (title, body):
        assert not any(c in text for c in "\x1b\x07\r\x00‮​")
    assert "\n" not in title
    assert body.count("\n") == 1  # only the deliberate line break before "Server and local copies"
    assert body.endswith("\nServer and local copies have diverged.")
