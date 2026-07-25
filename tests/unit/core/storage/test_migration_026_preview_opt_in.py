from __future__ import annotations

import io
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, text
from sqlalchemy import inspect as sa_inspect

from core.preview.storage_availability import (
    CFG_PREVIEW_EVIDENCE_ENABLED,
    PreviewEvidenceIssueKind,
)
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule

EVIDENCE_NAMES = {
    "ccloud_cost_source_records",
    "ccloud_allocation_lineage_runs",
    "ccloud_allocation_lineage_portions",
    "ccloud_source_evidence_attempts",
    "ccloud_source_capture_readiness",
    "ccloud_organization_authority_attempts",
}


def _config(url: str, *, selection: str | None = None, output: io.StringIO | None = None) -> Config:
    migrations = Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
    config = Config(str(migrations / "alembic.ini"), output_buffer=output)
    config.set_main_option("script_location", str(migrations))
    config.set_main_option("sqlalchemy.url", url)
    config.cmd_opts = SimpleNamespace(x=[] if selection is None else [f"focus_preview={selection}"])
    return config


def _table_names(url: str) -> set[str]:
    engine = create_engine(url)
    try:
        return set(sa_inspect(engine).get_table_names())
    finally:
        engine.dispose()


def test_backend_disabled_migration_to_head_creates_no_preview_evidence(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'disabled-migrated.db'}"
    backend = SQLModelBackend(
        url,
        CCloudStorageModule(),
        focus_preview_enabled=False,
    )
    backend.create_tables()

    assert not (_table_names(url) & EVIDENCE_NAMES)
    assert "ccloud_billing" in _table_names(url)
    backend.dispose()


def test_backend_enabled_migration_to_head_creates_complete_preview_evidence(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'enabled-migrated.db'}"
    backend = SQLModelBackend(
        url,
        CCloudStorageModule(),
        focus_preview_enabled=True,
    )
    backend.create_tables()

    assert _table_names(url) >= EVIDENCE_NAMES
    assert backend.preview_evidence_availability.state.value == "ready"
    backend.dispose()


@pytest.mark.parametrize("selection", [None, "disabled"])
def test_direct_disabled_offline_upgrade_through_head_requires_online_preflight(
    selection: str | None,
) -> None:
    output = io.StringIO()
    config = _config("sqlite:///offline.db", selection=selection, output=output)
    assert ScriptDirectory.from_config(config).get_current_head() == "030"
    with pytest.raises(
        RuntimeError,
        match=r"migration 029 requires an online database connection.*without --sql",
    ):
        command.upgrade(config, "025:head", sql=True)

    sql = output.getvalue().lower()
    assert all(name not in sql for name in EVIDENCE_NAMES)


def test_direct_enabled_offline_upgrade_fails_before_bind_access() -> None:
    availability = __import__("core.preview.storage_availability", fromlist=["PreviewEvidenceOfflineMigrationError"])

    with pytest.raises(availability.PreviewEvidenceOfflineMigrationError) as raised:
        command.upgrade(
            _config("sqlite:///offline.db", selection="confluent_cloud", output=io.StringIO()),
            "025:head",
            sql=True,
        )

    assert str(raised.value) == (
        "Preview evidence migrations require an online database connection; run "
        "`uv run alembic -c src/core/storage/migrations/alembic.ini "
        "-x focus_preview=confluent_cloud upgrade head`."
    )


def test_direct_unknown_preview_selection_fails_before_migration(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="focus_preview"):
        command.upgrade(
            _config(f"sqlite:///{tmp_path / 'unknown.db'}", selection="unknown"),
            "head",
        )

    assert not (tmp_path / "unknown.db").exists()


@pytest.mark.parametrize(
    ("direct_selection", "backend_enabled"),
    [("confluent_cloud", False), ("disabled", True)],
)
def test_direct_selection_conflicting_with_backend_attribute_fails_before_migration(
    tmp_path: Path,
    direct_selection: str,
    backend_enabled: bool,
) -> None:
    database = tmp_path / f"conflict-{direct_selection}.db"
    config = _config(f"sqlite:///{database}", selection=direct_selection)
    config.attributes[CFG_PREVIEW_EVIDENCE_ENABLED] = backend_enabled

    with pytest.raises(ValueError, match="conflicts with backend configuration"):
        command.upgrade(config, "head")

    assert not database.exists()


def test_enable_after_revision_020_repairs_all_skipped_preview_evidence(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'enable-later.db'}"
    command.upgrade(_config(url, selection="disabled"), "020")
    assert not (_table_names(url) & EVIDENCE_NAMES)

    backend = SQLModelBackend(
        url,
        CCloudStorageModule(),
        focus_preview_enabled=True,
    )
    backend.create_tables()

    assert _table_names(url) >= EVIDENCE_NAMES
    assert backend.preview_evidence_availability.state.value == "ready"
    backend.dispose()


def test_direct_enabled_online_upgrade_selects_ccloud_preview_module(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'direct-enabled.db'}"

    command.upgrade(_config(url, selection="confluent_cloud"), "head")

    assert _table_names(url) >= EVIDENCE_NAMES


def test_enabled_backend_repairs_missing_optional_table_and_index(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'repair.db'}"
    backend = SQLModelBackend(url, CCloudStorageModule(), focus_preview_enabled=True)
    backend.create_tables()
    backend.dispose()
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(text("DROP TABLE ccloud_organization_authority_attempts"))
        connection.execute(text("DROP INDEX ix_ccloud_cost_source_retention"))
    engine.dispose()

    repaired = SQLModelBackend(url, CCloudStorageModule(), focus_preview_enabled=True)
    repaired.create_tables()

    assert repaired.preview_evidence_availability.state.value == "ready"
    assert "ccloud_organization_authority_attempts" in _table_names(url)
    check_engine = create_engine(url)
    try:
        indexes = {item["name"] for item in sa_inspect(check_engine).get_indexes("ccloud_cost_source_records")}
    finally:
        check_engine.dispose()
    assert "ix_ccloud_cost_source_retention" in indexes
    repaired.dispose()


def test_online_downgrade_removes_dormant_preview_objects_even_when_selection_is_disabled(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'downgrade.db'}"
    command.upgrade(_config(url, selection="confluent_cloud"), "head")
    assert _table_names(url) >= EVIDENCE_NAMES

    command.downgrade(_config(url, selection="disabled"), "017")

    assert not (_table_names(url) & EVIDENCE_NAMES)


@pytest.mark.parametrize(
    ("revision_range", "online_target"),
    [("026:025", "025"), ("021:020", "020"), ("018:017", "017")],
)
def test_offline_downgrade_across_preview_revisions_fails_before_partial_sql(
    revision_range: str,
    online_target: str,
) -> None:
    output = io.StringIO()

    availability = __import__("core.preview.storage_availability", fromlist=["PreviewEvidenceOfflineMigrationError"])
    with pytest.raises(availability.PreviewEvidenceOfflineMigrationError) as raised:
        command.downgrade(
            _config("sqlite:///offline.db", selection="disabled", output=output),
            revision_range,
            sql=True,
        )

    assert str(raised.value) == (
        "Preview evidence downgrades require an online database connection; run "
        "`uv run alembic -c src/core/storage/migrations/alembic.ini "
        f"-x focus_preview=confluent_cloud downgrade {online_target}`."
    )
    assert all(name not in output.getvalue().lower() for name in EVIDENCE_NAMES)


def _replace_organization_authority_table(url: str, ddl: str) -> None:
    engine = create_engine(url)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("DROP TABLE ccloud_organization_authority_attempts")
            connection.exec_driver_sql(ddl)
    finally:
        engine.dispose()


_ORGANIZATION_COLUMNS = """
    attempt_sequence INTEGER,
    ecosystem VARCHAR NOT NULL,
    tenant_id VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    started_at DATETIME NOT NULL,
    completed_at DATETIME,
    organization_id VARCHAR,
    failure_reason VARCHAR
"""


@pytest.mark.parametrize(
    "ddl",
    [
        """CREATE TABLE ccloud_organization_authority_attempts (
            attempt_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            ecosystem VARCHAR NOT NULL,
            tenant_id VARCHAR NOT NULL,
            started_at DATETIME NOT NULL,
            completed_at DATETIME,
            organization_id VARCHAR,
            failure_reason VARCHAR
        )""",
        f"CREATE TABLE ccloud_organization_authority_attempts ({_ORGANIZATION_COLUMNS})",
        """CREATE TABLE ccloud_organization_authority_attempts (
            attempt_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            ecosystem VARCHAR NOT NULL,
            tenant_id VARCHAR NOT NULL,
            status INTEGER NOT NULL,
            started_at DATETIME NOT NULL,
            completed_at DATETIME,
            organization_id VARCHAR,
            failure_reason VARCHAR
        )""",
        """CREATE TABLE ccloud_organization_authority_attempts (
            attempt_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            ecosystem VARCHAR NOT NULL,
            tenant_id VARCHAR NOT NULL,
            status VARCHAR NOT NULL,
            started_at DATETIME NOT NULL,
            completed_at DATETIME,
            organization_id VARCHAR NOT NULL,
            failure_reason VARCHAR
        )""",
    ],
    ids=["missing-required", "wrong-primary-key", "wrong-type", "wrong-nullability"],
)
def test_real_incompatible_preview_schema_isolated_while_core_storage_remains_readable(
    tmp_path: Path,
    ddl: str,
) -> None:
    url = f"sqlite:///{tmp_path / 'corrupt.db'}"
    initial = SQLModelBackend(url, CCloudStorageModule(), focus_preview_enabled=True)
    initial.create_tables()
    initial.dispose()
    _replace_organization_authority_table(url, ddl)

    reopened = SQLModelBackend(url, CCloudStorageModule(), focus_preview_enabled=True)
    reopened.create_tables()

    availability = reopened.preview_evidence_availability
    assert availability.state.value == "unavailable"
    assert any(issue.kind is PreviewEvidenceIssueKind.SCHEMA_INCOMPATIBLE for issue in availability.issues)
    with reopened.create_read_only_unit_of_work() as uow:
        assert uow.pipeline_state.count_calculated("confluent_cloud", "tenant-1") == 0
    with reopened.create_preview_metadata_read_unit_of_work() as uow:
        assert (
            uow.revisions.get_current_for_owner(
                ecosystem="confluent_cloud",
                tenant_id="tenant-1",
                month_start=date(2026, 7, 1),
            )
            is None
        )
    reopened.dispose()


def test_incompatible_preview_schema_does_not_block_all_core_revisions_through_head(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'ten-step.db'}"
    command.upgrade(_config(url, selection="disabled"), "017")
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE ccloud_cost_source_records (ecosystem VARCHAR NOT NULL)")
    engine.dispose()

    backend = SQLModelBackend(url, CCloudStorageModule(), focus_preview_enabled=True)
    backend.create_tables()

    version_engine = create_engine(url)
    try:
        with version_engine.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "030"
    finally:
        version_engine.dispose()
    assert backend.preview_evidence_availability.state.value == "unavailable"
    assert "pipeline_runs" in _table_names(url)
    backend.dispose()


def test_enabled_disabled_reenabled_preserves_objects_and_revalidates_ready(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'reenabled.db'}"
    enabled = SQLModelBackend(url, CCloudStorageModule(), focus_preview_enabled=True)
    enabled.create_tables()
    assert enabled.preview_evidence_availability.state.value == "ready"
    enabled.dispose()

    disabled = SQLModelBackend(url, CCloudStorageModule(), focus_preview_enabled=False)
    disabled.create_tables()
    assert disabled.preview_evidence_availability.state.value == "unavailable"
    assert _table_names(url) >= EVIDENCE_NAMES
    disabled.dispose()

    reenabled = SQLModelBackend(url, CCloudStorageModule(), focus_preview_enabled=True)
    reenabled.create_tables()
    assert reenabled.preview_evidence_availability.state.value == "ready"
    assert _table_names(url) >= EVIDENCE_NAMES
    reenabled.dispose()
