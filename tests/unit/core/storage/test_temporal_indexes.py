from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

from sqlalchemy import inspect as sa_inspect
from sqlalchemy import text
from sqlmodel import SQLModel, create_engine

from core.storage.backends.sqlmodel.base_tables import (  # noqa: F401 — registers SQLModel metadata
    IdentityTable,
    ResourceTable,
)

if TYPE_CHECKING:
    from alembic.config import Config

MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"

EXPECTED_RESOURCE_INDEXES = {"ix_resources_created_at", "ix_resources_deleted_at"}
EXPECTED_IDENTITY_INDEXES = {"ix_identities_created_at", "ix_identities_deleted_at"}


def _get_alembic_cfg(conn: str) -> Config:
    from alembic.config import Config

    alembic_ini = MIGRATIONS_DIR / "alembic.ini"
    cfg = Config(str(alembic_ini))
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", conn)
    return cfg


class TestTemporalIndexes:
    def test_all_four_temporal_indexes_present(self) -> None:
        """All 4 temporal indexes must exist on both tables after create_all."""
        engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(engine)
        inspector = sa_inspect(engine)

        resource_indexes = {idx["name"] for idx in inspector.get_indexes("resources")}
        identity_indexes = {idx["name"] for idx in inspector.get_indexes("identities")}

        missing = (EXPECTED_RESOURCE_INDEXES - resource_indexes) | (EXPECTED_IDENTITY_INDEXES - identity_indexes)
        assert not missing, f"Missing temporal indexes: {missing}"
        engine.dispose()

    def test_migration_006_upgrade_adds_temporal_indexes(self, tmp_path: pathlib.Path) -> None:
        """Migration 006 upgrade must create all 4 temporal indexes."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "006")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        resource_indexes = {idx["name"] for idx in inspector.get_indexes("resources")}
        identity_indexes = {idx["name"] for idx in inspector.get_indexes("identities")}
        engine.dispose()

        assert "ix_resources_created_at" in resource_indexes
        assert "ix_resources_deleted_at" in resource_indexes
        assert "ix_identities_created_at" in identity_indexes
        assert "ix_identities_deleted_at" in identity_indexes

    def test_migration_006_downgrade_removes_temporal_indexes(self, tmp_path: pathlib.Path) -> None:
        """Migration 006 downgrade must drop all 4 temporal indexes."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "006")
        command.downgrade(cfg, "ddebea2fe0a8")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        resource_indexes = {idx["name"] for idx in inspector.get_indexes("resources")}
        identity_indexes = {idx["name"] for idx in inspector.get_indexes("identities")}
        engine.dispose()

        assert "ix_resources_created_at" not in resource_indexes
        assert "ix_resources_deleted_at" not in resource_indexes
        assert "ix_identities_created_at" not in identity_indexes
        assert "ix_identities_deleted_at" not in identity_indexes

    def test_query_plan_uses_index_for_temporal_filter(self, tmp_path: pathlib.Path) -> None:
        """EXPLAIN QUERY PLAN must use index search, not full scan, for temporal filter."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "006")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(
                text(
                    "EXPLAIN QUERY PLAN "
                    "SELECT * FROM resources "
                    "WHERE ecosystem='x' AND tenant_id='y' "
                    "AND (created_at IS NULL OR created_at <= '2026-01-01T00:00:00') "
                    "AND (deleted_at IS NULL OR deleted_at > '2026-01-01T00:00:00')"
                )
            ).fetchall()
        engine.dispose()

        plan_text = " ".join(str(r) for r in rows)
        assert "SCAN resources" not in plan_text, f"Query plan uses full table scan instead of index: {plan_text}"
        assert any(
            name in plan_text
            for name in (
                "ix_resources_created_at",
                "ix_resources_deleted_at",
                "SEARCH resources",
            )
        ), f"Query plan does not use any temporal index: {plan_text}"
