from __future__ import annotations

import importlib
import shutil
from pathlib import Path
from typing import cast

import psycopg
import pytest

from tests.conftest import regenerate_generated_packages
from tests.test_result_matrix import RESULT_MATRIX_PACKAGE


def test_generated_package_setup_failure_removes_database_and_restores_namespace(
    pg_dsn: str,
    tmp_path: Path,
) -> None:
    generated_root = tmp_path / f"{RESULT_MATRIX_PACKAGE}_generated"
    shutil.copytree(Path(__file__).parent / "generated", generated_root)
    for schema_path in generated_root.glob("*/schema.sql"):
        schema_path.write_text(
            "DO $$ BEGIN RAISE EXCEPTION '%', current_database(); END $$;\n",
            encoding="utf-8",
        )

    generated_namespace = cast(
        "dict[str, object]",
        vars(importlib.import_module("tests.generated")),
    )
    original_search_locations = generated_namespace["__path__"]
    setup = regenerate_generated_packages(pg_dsn, generated_root)

    with pytest.raises(psycopg.errors.RaiseException) as error_info:
        next(setup)

    database_name = error_info.value.diag.message_primary
    assert database_name is not None
    with psycopg.connect(pg_dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS (SELECT FROM pg_database WHERE datname = %s)",
            (database_name,),
        )
        database_exists = cur.fetchone()
    assert database_exists == (False,)
    assert generated_namespace["__path__"] == original_search_locations
