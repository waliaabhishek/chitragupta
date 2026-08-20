from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text

from core.preview.storage_availability import PreviewEvidenceSchemaError
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
from plugins.confluent_cloud.storage.module import CCloudStorageModule
from tests.unit.core.storage.test_migration_027_historical_repair import _config

CATALOG_TABLE = "preview_artifact_files"


def test_revision_028_defines_preview_artifact_catalog_migration_in_current_chain() -> None:
    config = _config("sqlite:///unused.db")
    script = ScriptDirectory.from_config(config)
    migration_path = (
        Path(__file__).resolve().parents[4]
        / "src"
        / "core"
        / "storage"
        / "migrations"
        / "versions"
        / "028_add_preview_artifact_file_catalog.py"
    )

    assert script.get_current_head() == "033"
    source = migration_path.read_text(encoding="utf-8")
    assert 'run_preview_evidence_step("028")' in source
    assert 'run_preview_evidence_downgrade_step("028")' in source


def test_enabled_upgrade_to_028_adds_nullable_legacy_columns_and_normalized_catalog(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'enabled.db'}"

    command.upgrade(_config(url, selection="confluent_cloud"), "head")

    engine = create_engine(url)
    try:
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        request_columns = {column["name"]: column for column in inspector.get_columns("preview_requests")}
        revision_columns = {column["name"]: column for column in inspector.get_columns("preview_revisions")}
    finally:
        engine.dispose()

    assert CATALOG_TABLE in tables
    assert request_columns["data_files_json"]["nullable"] is True
    assert revision_columns["file_metadata_json"]["nullable"] is True
    assert request_columns["artifact_file_count"]["nullable"] is True
    assert request_columns["artifact_file_catalog_sha256"]["nullable"] is True
    assert revision_columns["artifact_file_count"]["nullable"] is True
    assert revision_columns["artifact_file_catalog_sha256"]["nullable"] is True


def test_disabled_upgrade_to_028_does_not_create_preview_catalog(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'disabled.db'}"

    command.upgrade(_config(url, selection="disabled"), "head")

    engine = create_engine(url)
    try:
        assert CATALOG_TABLE not in inspect(engine).get_table_names()
    finally:
        engine.dispose()


def test_catalog_schema_uses_owner_qualified_package_identity_and_order(tmp_path: Path) -> None:
    url = f"sqlite:///{tmp_path / 'catalog.db'}"
    command.upgrade(_config(url, selection="confluent_cloud"), "head")
    engine = create_engine(url)
    try:
        inspector = inspect(engine)
        primary_key = inspector.get_pk_constraint(CATALOG_TABLE)
        unique_constraints = inspector.get_unique_constraints(CATALOG_TABLE)
        indexes = inspector.get_indexes(CATALOG_TABLE)
    finally:
        engine.dispose()

    assert primary_key["constrained_columns"] == [
        "ecosystem",
        "tenant_id",
        "package_kind",
        "package_id",
        "file_order",
    ]
    assert any(
        constraint["column_names"] == ["ecosystem", "tenant_id", "package_kind", "package_id", "name"]
        for constraint in unique_constraints
    )
    indexed_columns = {tuple(index["column_names"]) for index in indexes}
    assert (
        "ecosystem",
        "tenant_id",
        "package_kind",
        "package_id",
        "file_order",
    ) in indexed_columns
    assert (
        "ecosystem",
        "tenant_id",
        "package_kind",
        "package_id",
        "name",
    ) in indexed_columns


def test_fresh_core_schema_registers_catalog_and_matches_nullable_parent_contract(
    tmp_path: Path,
) -> None:
    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'fresh.db'}",
        CoreStorageModule(),
        use_migrations=False,
        focus_preview_enabled=True,
    )
    try:
        backend.create_tables()
        inspector = inspect(backend._engine)
        assert CATALOG_TABLE in inspector.get_table_names()
        request_columns = {column["name"]: column for column in inspector.get_columns("preview_requests")}
        revision_columns = {column["name"]: column for column in inspector.get_columns("preview_revisions")}
    finally:
        backend.dispose()

    assert request_columns["data_files_json"]["nullable"] is True
    assert revision_columns["file_metadata_json"]["nullable"] is True


def test_enabling_preview_after_disabled_head_repairs_revision_028_schema(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'late-opt-in.db'}"
    command.upgrade(_config(url, selection="disabled"), "head")

    backend = SQLModelBackend(url, CCloudStorageModule(), focus_preview_enabled=True)
    try:
        backend.create_tables()
        inspector = inspect(backend._engine)
        table_names = inspector.get_table_names()
        request_columns = {column["name"]: column for column in inspector.get_columns("preview_requests")}
        revision_columns = {column["name"]: column for column in inspector.get_columns("preview_revisions")}
    finally:
        backend.dispose()

    assert CATALOG_TABLE in table_names
    assert request_columns["artifact_file_count"]["nullable"] is True
    assert request_columns["artifact_file_catalog_sha256"]["nullable"] is True
    assert revision_columns["file_metadata_json"]["nullable"] is True
    assert revision_columns["artifact_file_count"]["nullable"] is True
    assert revision_columns["artifact_file_catalog_sha256"]["nullable"] is True


def _catalog_digest(item: dict[str, object]) -> str:
    encoded = json.dumps(item, sort_keys=True, separators=(",", ":")).encode()
    digest = hashlib.sha256()
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)
    return digest.hexdigest()


def test_downgrade_reconstructs_request_and_revision_legacy_json_before_catalog_drop(
    tmp_path: Path,
) -> None:
    url = f"sqlite:///{tmp_path / 'downgrade.db'}"
    config = _config(url, selection="confluent_cloud")
    command.upgrade(config, "head")
    item = {
        "name": "cost-and-usage.csv",
        "media_type": "text/csv",
        "size_bytes": 7,
        "sha256": "a" * 64,
        "order": 1,
    }
    digest = _catalog_digest(item)
    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO preview_requests
                    (request_id, tenant_name, ecosystem, tenant_id, grain, start_date,
                     end_date, column_profile, status, created_at, storage_key,
                     manifest_metadata_json, data_files_json, artifact_file_count,
                     artifact_file_catalog_sha256)
                VALUES
                    ('request-1', 'production', 'confluent_cloud', 'tenant-1', 'daily',
                     '2026-07-01', '2026-07-02', 'full', 'ready', '2026-07-03',
                     'request-storage', '{}', NULL, 1, :digest)
                """
            ),
            {"digest": digest},
        )
        connection.execute(
            text(
                """
                INSERT INTO preview_revisions
                    (revision_id, tenant_name_at_publication, ecosystem, tenant_id,
                     month_start, month_end, monthly_status, material_sha256,
                     source_snapshot_json, published_at, is_current, storage_key,
                     manifest_metadata_json, file_metadata_json, artifact_file_count,
                     artifact_file_catalog_sha256)
                VALUES
                    ('revision-1', 'production', 'confluent_cloud', 'tenant-1',
                     '2026-07-01', '2026-08-01', 'settled', :material, '{}',
                     '2026-08-03', 1, 'revision-storage', '{}', NULL, 1, :digest)
                """
            ),
            {"digest": digest, "material": "b" * 64},
        )
        for package_kind, package_id in (
            ("requested", "request-1"),
            ("revision", "revision-1"),
        ):
            connection.execute(
                text(
                    """
                    INSERT INTO preview_artifact_files
                        (ecosystem, tenant_id, package_kind, package_id, file_order,
                         name, media_type, size_bytes, sha256)
                    VALUES
                        ('confluent_cloud', 'tenant-1', :package_kind, :package_id, 1,
                         'cost-and-usage.csv', 'text/csv', 7, :sha256)
                    """
                ),
                {
                    "package_kind": package_kind,
                    "package_id": package_id,
                    "sha256": "a" * 64,
                },
            )
    engine.dispose()

    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE preview_revisions
                SET artifact_file_catalog_sha256 = :digest
                WHERE revision_id = 'revision-1'
                """
            ),
            {"digest": "0" * 64},
        )
    engine.dispose()
    with pytest.raises(PreviewEvidenceSchemaError):
        command.downgrade(config, "027")
    engine = create_engine(url)
    try:
        with engine.connect() as connection:
            version = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            request_json_after_failure = connection.execute(
                text("SELECT data_files_json FROM preview_requests WHERE request_id = 'request-1'")
            ).scalar_one_or_none()
        assert CATALOG_TABLE in inspect(engine).get_table_names()
    finally:
        engine.dispose()
    assert version == "028"
    assert request_json_after_failure is None

    engine = create_engine(url)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE preview_revisions
                SET artifact_file_catalog_sha256 = :digest
                WHERE revision_id = 'revision-1'
                """
            ),
            {"digest": digest},
        )
    engine.dispose()
    command.downgrade(config, "027")

    engine = create_engine(url)
    try:
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        request_columns = {column["name"]: column for column in inspector.get_columns("preview_requests")}
        revision_columns = {column["name"]: column for column in inspector.get_columns("preview_revisions")}
        with engine.connect() as connection:
            request_json = connection.execute(
                text("SELECT data_files_json FROM preview_requests WHERE request_id = 'request-1'")
            ).scalar_one()
            revision_json = connection.execute(
                text("SELECT file_metadata_json FROM preview_revisions WHERE revision_id = 'revision-1'")
            ).scalar_one()
    finally:
        engine.dispose()

    assert CATALOG_TABLE not in table_names
    assert "artifact_file_count" not in request_columns
    assert "artifact_file_count" not in revision_columns
    assert revision_columns["file_metadata_json"]["nullable"] is False
    assert json.loads(request_json) == [item]
    assert json.loads(revision_json) == [item]
