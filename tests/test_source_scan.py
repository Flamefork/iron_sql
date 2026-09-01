from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING
from typing import cast

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from collections.abc import Iterator
    from pathlib import Path

    from iron_sql.runtime import Query
    from tests.conftest import ProjectBuilder

_BASE_SQL = "SELECT 1 AS value"


def _write(root: Path, relative_path: str, source: str) -> None:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


# A refused directory holds another tree, such as an environment installation,
# a tool cache, or a second checkout. These tests pin what the scan refuses and
# what it accepts, because a broad rule can silently remove project statements.


def test_a_statement_inside_a_hidden_directory_is_not_collected(
    test_project: ProjectBuilder,
) -> None:
    hidden_sql = "SELECT 2 AS hidden_value"
    test_project.add_query("base", _BASE_SQL)
    _write(
        test_project.src_path,
        ".venv/lib/python3.13/site-packages/other/api.py",
        f"testdb_sql({hidden_sql!r})\n",
    )

    module = test_project.generate()
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(_BASE_SQL)
    with pytest.raises(KeyError, match="Unknown statement"):
        testdb_sql(hidden_sql)


def test_an_unparsable_file_inside_a_hidden_directory_does_not_abort_the_scan(
    test_project: ProjectBuilder,
) -> None:
    # Installed code can target another Python version. Refusing the directory
    # keeps that code from deciding whether this project can generate.
    test_project.add_query("base", _BASE_SQL)
    _write(
        test_project.src_path,
        ".cache/legacy.py",
        "testdb_sql(\ndef broken(\n",
    )

    module = test_project.generate()
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(_BASE_SQL)


def test_a_statement_inside_a_virtual_environment_is_not_collected(
    test_project: ProjectBuilder,
) -> None:
    environment_sql = "SELECT 3 AS environment_value"
    test_project.add_query("base", _BASE_SQL)
    _write(test_project.src_path, "environment/pyvenv.cfg", "home = /usr/bin\n")
    _write(
        test_project.src_path,
        "environment/lib/probe.py",
        f"testdb_sql({environment_sql!r})\n",
    )

    module = test_project.generate()
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(_BASE_SQL)
    with pytest.raises(KeyError, match="Unknown statement"):
        testdb_sql(environment_sql)


def test_an_unparsable_file_inside_a_virtual_environment_does_not_abort_the_scan(
    test_project: ProjectBuilder,
) -> None:
    test_project.add_query("base", _BASE_SQL)
    _write(test_project.src_path, "environment/pyvenv.cfg", "home = /usr/bin\n")
    _write(
        test_project.src_path,
        "environment/lib/legacy.py",
        "testdb_sql(\ndef broken(\n",
    )

    module = test_project.generate()
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(_BASE_SQL)


def test_the_scan_root_is_not_judged_by_the_rules_for_child_directories(
    test_project: ProjectBuilder,
) -> None:
    # The caller selects the scan root. Its name and pyvenv.cfg file cannot make
    # it a child tree that belongs to another owner.
    hidden_root = test_project.root / ".workspace"
    test_project.src_path.rename(hidden_root)
    test_project.src_path = hidden_root
    test_project.app_dir = hidden_root / test_project.app_pkg
    _write(hidden_root, "pyvenv.cfg", "home = /usr/bin\n")
    test_project.add_query("base", _BASE_SQL)

    module = test_project.generate()
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(_BASE_SQL)


def test_a_statement_in_a_directory_no_import_can_name_is_collected(
    test_project: ProjectBuilder,
) -> None:
    script_sql = "SELECT 4 AS script_value"
    _write(
        test_project.src_path,
        "ops-scripts/report.py",
        f"testdb_sql({script_sql!r})\n",
    )

    module = test_project.generate()
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(script_sql)


def test_a_statement_in_a_file_no_import_can_name_is_collected(
    test_project: ProjectBuilder,
) -> None:
    script_sql = "SELECT 5 AS script_value"
    _write(
        test_project.src_path,
        "app/my-script.py",
        f"testdb_sql({script_sql!r})\n",
    )

    module = test_project.generate()
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(script_sql)


def test_a_directory_without_an_init_file_is_scanned(
    test_project: ProjectBuilder,
) -> None:
    namespace_sql = "SELECT 6 AS namespace_value"
    _write(
        test_project.src_path,
        "namespace/nested/queries.py",
        f"testdb_sql({namespace_sql!r})\n",
    )

    module = test_project.generate()
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(namespace_sql)


def test_statement_locations_use_whole_tree_order(
    test_project: ProjectBuilder,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ordered_sql = "SELECT 7 AS ordered_value"
    middle_sql = "SELECT 8 AS middle_value"
    _write(test_project.src_path, "app/a.py", f"testdb_sql({ordered_sql!r})\n")
    _write(
        test_project.src_path,
        "tests/test_a.py",
        f"testdb_sql({ordered_sql!r})\n",
    )
    _write(test_project.src_path, "middle.py", f"testdb_sql({middle_sql!r})\n")

    path_type = type(test_project.src_path)
    original_glob = path_type.glob
    original_walk = path_type.walk

    def reverse_glob(path: Path, pattern: str) -> Iterator[Path]:
        return iter(reversed(tuple(original_glob(path, pattern))))

    def reverse_walk(path: Path) -> Iterator[tuple[Path, list[str], list[str]]]:
        for directory, subdirs, names in original_walk(path):
            subdirs.reverse()
            names.reverse()
            yield directory, subdirs, names

    monkeypatch.setattr(path_type, "glob", reverse_glob)
    monkeypatch.setattr(path_type, "walk", reverse_walk)

    test_project.generate_no_import()

    generated_path = (
        test_project.src_path / f"{test_project.module_full_name.replace('.', '/')}.py"
    )
    generated = generated_path.read_text(encoding="utf-8")
    assert re.findall(r"_locations = (.+)", generated) == [
        "('app/a.py:1', 'tests/test_a.py:1')",
        "('middle.py:1',)",
    ]


def test_a_symlinked_directory_is_not_scanned(
    test_project: ProjectBuilder,
) -> None:
    linked_sql = "SELECT 8 AS linked_value"
    test_project.add_query("base", _BASE_SQL)
    outside = test_project.root / "outside"
    _write(outside, "queries.py", f"testdb_sql({linked_sql!r})\n")
    (test_project.src_path / "linked").symlink_to(
        outside,
        target_is_directory=True,
    )
    debug_path = test_project.root / "debug"

    module = test_project.generate(debug_path=debug_path)
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(_BASE_SQL)
    with pytest.raises(KeyError, match="Unknown statement"):
        testdb_sql(linked_sql)
    assert json.loads(
        (debug_path / "skipped_dirs.json").read_text(encoding="utf-8")
    ) == [{"location": "linked", "reason": "symbolic link"}]


def test_a_symlinked_python_file_is_scanned(
    test_project: ProjectBuilder,
) -> None:
    linked_sql = "SELECT 9 AS linked_value"
    outside = test_project.root / "outside"
    _write(outside, "queries.py", f"testdb_sql({linked_sql!r})\n")
    (test_project.src_path / "linked.py").symlink_to(outside / "queries.py")
    debug_path = test_project.root / "debug"

    module = test_project.generate(debug_path=debug_path)
    testdb_sql = cast("Callable[[str], Query[object]]", vars(module)["testdb_sql"])

    testdb_sql(linked_sql)
    assert (
        json.loads((debug_path / "skipped_dirs.json").read_text(encoding="utf-8")) == []
    )


def test_skipped_directories_are_written_to_the_debug_directory(
    test_project: ProjectBuilder,
) -> None:
    # The report does not inspect refused contents. It records only the path and
    # the reason that the walk already knows.
    test_project.add_query("base", _BASE_SQL)
    _write(test_project.src_path, ".venv/lib/probe.py", "")
    _write(test_project.src_path, "environment/pyvenv.cfg", "home = /usr/bin\n")
    _write(test_project.src_path, "environment/lib/probe.py", "")
    debug_path = test_project.root / "debug"

    test_project.generate_no_import(debug_path=debug_path)

    skipped_path = debug_path / "skipped_dirs.json"
    assert json.loads(skipped_path.read_text(encoding="utf-8")) == [
        {"location": ".venv", "reason": "hidden directory"},
        {"location": "environment", "reason": "virtual environment"},
    ]

    (test_project.src_path / ".venv").rename(test_project.src_path / "cache")
    (test_project.src_path / "environment" / "pyvenv.cfg").unlink()
    test_project.generate_no_import(debug_path=debug_path)

    assert json.loads(skipped_path.read_text(encoding="utf-8")) == []
