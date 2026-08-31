import os
import subprocess
import sys
from pathlib import Path

import psycopg


def test_full_example(pg_test_dsn: str) -> None:
    project_root = Path(__file__).parent.parent

    with psycopg.connect(pg_test_dsn, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
        conn.execute("GRANT ALL ON SCHEMA public TO public")

    environment = {**os.environ, "DATABASE_URL": pg_test_dsn}
    generation = subprocess.run(
        [
            sys.executable,
            "-c",
            """\
import os
from pathlib import Path
from example.generate import generate_db_module, init_db
dsn = os.environ["DATABASE_URL"]
src_path = Path("example")
schema_path = Path("schema.sql")
init_db(dsn, src_path / schema_path)
print(generate_db_module(dsn, schema_path, src_path))
""",
        ],
        cwd=project_root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert generation.returncode == 0, generation.stderr
    assert generation.stdout == "False\n"

    completed = subprocess.run(
        [sys.executable, "-m", "example.main"],
        cwd=project_root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Users: 1" in completed.stdout
    assert "User: alice (alice@example.com)" in completed.stdout
    assert "All tasks: 2" in completed.stdout
    assert "Open tasks: 1" in completed.stdout
    assert "User not found (expected)" in completed.stdout
    assert "Streamed task: Set up CI (in_progress)" in completed.stdout
    assert "Audit (separate conn): 1 in-progress tasks" in completed.stdout
    assert completed.stdout.rstrip().splitlines()[-1].startswith("Received: ")
