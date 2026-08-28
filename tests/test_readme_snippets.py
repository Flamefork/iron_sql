import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import LiteralString
from typing import cast

import psycopg

from tests.conftest import BasedPyrightReport


def test_getting_started_blocks_generate_and_typecheck(
    tmp_path: Path,
    pg_test_dsn: str,
) -> None:
    project_root = Path(__file__).parent.parent
    readme = (project_root / "README.md").read_text(encoding="utf-8")
    section = readme.split("\n## Getting Started\n", maxsplit=1)[1].split(
        "\n## Customization\n", maxsplit=1
    )[0]
    blocks = [
        match.group(1)
        for match in re.finditer(r"```python\n(.*?)```", section, re.DOTALL)
    ]
    assert len(blocks) == 2
    query_block, generation_block = [textwrap.dedent(block) for block in blocks]

    schema = cast(
        "LiteralString",
        """
        CREATE TABLE users (
            id UUID PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        """,
    )
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
    (tmp_path / "schema.sql").write_text(schema, encoding="utf-8")
    (package / "queries.py").write_text(
        "\n".join([
            "import uuid",
            "",
            "",
            "async def readme_usage(uid: uuid.UUID) -> None:",
            textwrap.indent(query_block.rstrip(), "    "),
            "    reveal_type(user)",
            "",
        ]),
        encoding="utf-8",
    )
    (tmp_path / "generate.py").write_text(
        generation_block.rstrip() + "\n", encoding="utf-8"
    )
    shutil.copy(project_root / "pyproject.toml", tmp_path / "pyproject.toml")

    completed = subprocess.run(
        [sys.executable, "generate.py"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert (package / "db/mydb.py").exists()

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
    assert [
        diagnostic.message
        for diagnostic in report.general_diagnostics
        if diagnostic.severity == "information"
    ] == ['Type of "user" is "MydbUser"']
    assert report.summary.files_analyzed >= 4
