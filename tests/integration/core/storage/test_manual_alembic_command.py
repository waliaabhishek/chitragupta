from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import make_url

TEST_POSTGRESQL_URL = os.environ.get("TEST_POSTGRESQL_URL")
PROJECT_ROOT = Path(__file__).resolve().parents[4]
MIGRATIONS_DIR = PROJECT_ROOT / "src" / "core" / "storage" / "migrations"
ALEMBIC_INI = MIGRATIONS_DIR / "alembic.ini"
PREVIEW_EVIDENCE_TABLES = {
    "ccloud_cost_source_records",
    "ccloud_allocation_lineage_runs",
    "ccloud_allocation_lineage_portions",
    "ccloud_source_evidence_attempts",
    "ccloud_source_capture_readiness",
    "ccloud_organization_authority_attempts",
}


def _run_manual_alembic(cwd: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "uv",
            "run",
            "--project",
            str(PROJECT_ROOT),
            "alembic",
            "-c",
            str(ALEMBIC_INI),
            *args,
        ],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def _sqlite_tables(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(inspect(engine).get_table_names())
    finally:
        engine.dispose()


@pytest.fixture
def postgresql_role_schema_url() -> Iterator[str]:
    if TEST_POSTGRESQL_URL is None:
        pytest.skip("TEST_POSTGRESQL_URL is not configured")

    role = f"task_254_51_role_{uuid4().hex[:16]}"
    schema = f"task_254_51_schema_{uuid4().hex}"
    password = "p@ss=word"  # pragma: allowlist secret
    admin_engine = create_engine(TEST_POSTGRESQL_URL)
    admin_url = make_url(TEST_POSTGRESQL_URL)
    database = admin_url.database
    assert database is not None
    with admin_engine.begin() as connection:
        connection.exec_driver_sql(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        connection.exec_driver_sql(f'GRANT CONNECT ON DATABASE "{database}" TO "{role}"')
        connection.exec_driver_sql(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role}"')

    role_url = (
        admin_url.set(username=role, password=password)
        .update_query_dict({"options": f"-csearch_path={schema}"})
        .render_as_string(hide_password=False)
    )
    try:
        yield role_url
    finally:
        with admin_engine.begin() as connection:
            connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            connection.exec_driver_sql(f'REVOKE CONNECT ON DATABASE "{database}" FROM "{role}"')
            connection.exec_driver_sql(f'DROP ROLE IF EXISTS "{role}"')
        admin_engine.dispose()


def test_manual_sqlalchemy_url_x_argument_targets_requested_sqlite_database(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.db"
    default_sqlite = tmp_path / "data" / "chargeback.db"
    default_sqlite.parent.mkdir()

    completed = _run_manual_alembic(
        tmp_path,
        "-x",
        f"sqlalchemy.url=sqlite:///{target}",
        "upgrade",
        "001",
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert target.exists()
    assert "alembic_version" in _sqlite_tables(f"sqlite:///{target}")
    assert "billing" in _sqlite_tables(f"sqlite:///{target}")
    assert not default_sqlite.exists()


@pytest.mark.skipif(TEST_POSTGRESQL_URL is None, reason="TEST_POSTGRESQL_URL is not configured")
def test_manual_sqlalchemy_url_x_argument_targets_requested_postgresql_schema(
    tmp_path: Path,
    postgresql_role_schema_url: str,
) -> None:
    default_sqlite = tmp_path / "data" / "chargeback.db"
    default_sqlite.parent.mkdir()

    assert "p%40ss%3Dword" in postgresql_role_schema_url  # pragma: allowlist secret
    assert "options=-csearch_path%3D" in postgresql_role_schema_url

    completed = _run_manual_alembic(
        tmp_path,
        "-x",
        f"sqlalchemy.url={postgresql_role_schema_url}",
        "upgrade",
        "001",
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    engine = create_engine(postgresql_role_schema_url)
    try:
        inspector = inspect(engine)
        assert "alembic_version" in inspector.get_table_names()
        assert "billing" in inspector.get_table_names()
    finally:
        engine.dispose()
    assert not default_sqlite.exists()


def test_manual_sqlalchemy_url_x_argument_invalid_override_fails_without_fallback_or_secret_echo(
    tmp_path: Path,
) -> None:
    default_sqlite = tmp_path / "data" / "chargeback.db"
    default_sqlite.parent.mkdir()
    invalid_override = "postgresql+psycopg://user:secret-token@example.invalid:bad/db"  # pragma: allowlist secret

    completed = _run_manual_alembic(
        tmp_path,
        "-x",
        f"sqlalchemy.url={invalid_override}",
        "upgrade",
        "001",
    )

    combined = completed.stdout + completed.stderr
    assert completed.returncode != 0, combined
    assert "invalid sqlalchemy.url override" in combined
    assert "secret-token" not in combined  # pragma: allowlist secret
    assert invalid_override not in combined  # pragma: allowlist secret
    assert not default_sqlite.exists()


def test_manual_sqlalchemy_url_x_argument_preserves_focus_preview_selection(
    tmp_path: Path,
) -> None:
    target = tmp_path / "preview-enabled.db"
    default_sqlite = tmp_path / "data" / "chargeback.db"
    default_sqlite.parent.mkdir()

    completed = _run_manual_alembic(
        tmp_path,
        "-x",
        f"sqlalchemy.url=sqlite:///{target}",
        "-x",
        "focus_preview=confluent_cloud",
        "upgrade",
        "head",
    )

    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert target.exists()
    assert _sqlite_tables(f"sqlite:///{target}") >= PREVIEW_EVIDENCE_TABLES
    assert not default_sqlite.exists()
