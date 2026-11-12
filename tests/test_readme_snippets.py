import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import LiteralString

import psycopg

from tests.conftest import BasedPyrightReport

PROJECT_ROOT = Path(__file__).parent.parent


def _readme_blocks(section: str, next_section: str) -> list[str]:
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    text = readme.split(f"\n## {section}\n", maxsplit=1)[1].split(
        f"\n## {next_section}\n", maxsplit=1
    )[0]
    return [
        textwrap.dedent(match.group(1))
        for match in re.finditer(r"```python\n(.*?)```", text, re.DOTALL)
    ]


def _create_project(
    tmp_path: Path, pg_test_dsn: str, schema: LiteralString, queries: str
) -> Path:
    with psycopg.connect(pg_test_dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
        conn.execute("GRANT ALL ON SCHEMA public TO public")
        conn.execute(schema)

    package = tmp_path / "myapp"
    (package / "db").mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "db/__init__.py").write_text("", encoding="utf-8")
    (package / "config.py").write_text(f"DSN = {pg_test_dsn!r}\n", encoding="utf-8")
    (package / "queries.py").write_text(queries, encoding="utf-8")
    (tmp_path / "schema.sql").write_text(schema, encoding="utf-8")
    shutil.copy(PROJECT_ROOT / "pyproject.toml", tmp_path / "pyproject.toml")
    return package


def _generate_and_typecheck(tmp_path: Path, generation: str) -> BasedPyrightReport:
    (tmp_path / "generate.py").write_text(generation, encoding="utf-8")
    completed = subprocess.run(
        [sys.executable, "generate.py"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert (tmp_path / "myapp/db/mydb.py").exists()

    typecheck = subprocess.run(
        [
            sys.executable,
            "-m",
            "basedpyright",
            "--outputjson",
            "--pythonpath",
            sys.executable,
            ".",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    report = BasedPyrightReport.model_validate_json(typecheck.stdout)
    assert [
        diagnostic
        for diagnostic in report.general_diagnostics
        if diagnostic.severity == "error"
    ] == []
    return report


def _information(report: BasedPyrightReport) -> list[str]:
    return [
        diagnostic.message
        for diagnostic in report.general_diagnostics
        if diagnostic.severity == "information"
    ]


def test_getting_started_blocks_generate_and_typecheck(
    tmp_path: Path,
    pg_test_dsn: str,
) -> None:
    query_block, generation_block = _readme_blocks("Getting Started", "Customization")
    _create_project(
        tmp_path,
        pg_test_dsn,
        """
        CREATE TABLE users (
            id UUID PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
        "\n".join([
            "import uuid",
            "",
            "",
            "async def readme_usage(uid: uuid.UUID) -> None:",
            textwrap.indent(query_block.rstrip(), "    "),
            "    reveal_type(user)",
            "",
        ]),
    )

    report = _generate_and_typecheck(tmp_path, generation_block)

    assert _information(report) == ['Type of "user" is "MydbUser"']
    assert report.summary.files_analyzed >= 4


def test_json_columns_blocks_generate_and_typecheck(
    tmp_path: Path,
    pg_test_dsn: str,
) -> None:
    models_block, query_block, generation_block = _readme_blocks(
        "JSON Columns", "Runtime Highlights"
    )
    package = _create_project(
        tmp_path,
        pg_test_dsn,
        """
        CREATE TABLE projects (
            id UUID PRIMARY KEY,
            settings JSONB NOT NULL DEFAULT '{}'
        );

        CREATE TABLE tasks (
            id UUID PRIMARY KEY,
            project_id UUID NOT NULL REFERENCES projects(id),
            title TEXT NOT NULL
        );
        """,
        "\n".join([
            "from myapp.db.mydb import mydb_sql",
            "",
            "",
            "async def readme_usage() -> None:",
            textwrap.indent(query_block.rstrip(), "    "),
            "    reveal_type(projects[0].settings)",
            "    reveal_type(projects[0].tasks)",
            "",
        ]),
    )
    (package / "models.py").write_text(models_block, encoding="utf-8")

    report = _generate_and_typecheck(
        tmp_path,
        (
            "from pathlib import Path\n\n"
            "from iron_sql.codegen import render_sql_module\n\n"
            f"{generation_block}"
        ),
    )

    assert _information(report) == [
        'Type of "projects[0].settings" is "ProjectSettings"',
        'Type of "projects[0].tasks" is "list[Task] | None"',
    ]
