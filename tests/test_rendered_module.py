import os
from pathlib import Path

import pytest

from iron_sql.codegen import RenderedModule

_TEXT = "x = 1\ny = 2\n"


@pytest.mark.parametrize(
    ("disk", "text"),
    [
        pytest.param(None, _TEXT, id="missing"),
        pytest.param(_TEXT, _TEXT, id="current"),
        pytest.param("x = 1\ny = 3\n", _TEXT, id="changed"),
        pytest.param("x = 1\ny = 2", _TEXT, id="no-final-newline"),
        pytest.param(_TEXT + "\n", _TEXT, id="extra-final-newline"),
        pytest.param("x = 1\r\ny = 2\r\n", _TEXT, id="crlf"),
        pytest.param(None, "", id="missing-empty"),
    ],
)
def test_diff_is_empty_exactly_when_disk_matches_text(
    tmp_path: Path, disk: str | None, text: str
) -> None:
    path = tmp_path / "pkg" / "mod.py"
    if disk is not None:
        path.parent.mkdir()
        path.write_bytes(disk.encode())
    rendered = RenderedModule(path, text)

    diff = rendered.diff()

    assert (path.read_bytes().decode() if path.exists() else None) == disk
    assert (diff == "") is (disk == text)
    rendered.write()
    assert path.read_bytes() == text.encode()
    assert rendered.diff() == ""


def test_diff_marks_missing_file_as_created(tmp_path: Path) -> None:
    path = tmp_path / "mod.py"

    assert RenderedModule(path, _TEXT).diff() == "".join([
        "--- /dev/null\n",
        f"+++ {path}\n",
        "@@ -0,0 +1,2 @@\n",
        "+x = 1\n",
        "+y = 2\n",
    ])


def test_diff_marks_missing_final_newline(tmp_path: Path) -> None:
    path = tmp_path / "mod.py"
    path.write_text("x = 1\ny = 2", encoding="utf-8")

    assert RenderedModule(path, _TEXT).diff() == "".join([
        f"--- {path}\n",
        f"+++ {path}\n",
        "@@ -1,2 +1,2 @@\n",
        " x = 1\n",
        "-y = 2\n",
        "\\ No newline at end of file\n",
        "+y = 2\n",
    ])


def test_diff_splits_lines_only_at_newline(tmp_path: Path) -> None:
    path = tmp_path / "mod.py"
    path.write_text("s = 'a\u2028b'\nx = 1\n", encoding="utf-8")

    assert RenderedModule(path, "s = 'a\u2028c'\nx = 1\n").diff() == "".join([
        f"--- {path}\n",
        f"+++ {path}\n",
        "@@ -1,2 +1,2 @@\n",
        "-s = 'a\u2028b'\n",
        "+s = 'a\u2028c'\n",
        " x = 1\n",
    ])


def test_write_keeps_current_file_untouched(tmp_path: Path) -> None:
    path = tmp_path / "mod.py"
    path.write_text(_TEXT, encoding="utf-8")
    os.utime(path, ns=(1_000_000_000, 1_000_000_000))

    RenderedModule(path, _TEXT).write()

    assert path.stat().st_mtime_ns == 1_000_000_000
