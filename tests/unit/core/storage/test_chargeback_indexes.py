from __future__ import annotations

import pathlib

import pytest
from sqlalchemy import Index
from sqlalchemy import inspect as sa_inspect
from sqlmodel import SQLModel, create_engine

from core.storage.backends.sqlmodel.tables import (
    ChargebackDimensionTable,
    ChargebackFactTable,
)

MIGRATIONS_DIR = pathlib.Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"

FACT_INDEX_NAME = "ix_chargeback_facts_dimension_timestamp"
DIM_INDEX_NAME = "ix_chargeback_dimensions_eco_tenant"


def _get_alembic_cfg(conn: str):
    from alembic.config import Config

    alembic_ini = MIGRATIONS_DIR / "alembic.ini"
    cfg = Config(str(alembic_ini))
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", conn)
    return cfg


class TestChargebackFactTableArgs:
    def test_table_args_has_dimension_timestamp_index(self) -> None:
        """ChargebackFactTable.__table_args__ must contain Index named ix_chargeback_facts_dimension_timestamp."""
        table_args = getattr(ChargebackFactTable, "__table_args__", None)
        assert table_args is not None, "ChargebackFactTable must have __table_args__"

        index_names = {arg.name for arg in table_args if isinstance(arg, Index)}
        assert FACT_INDEX_NAME in index_names, (
            f"Expected Index '{FACT_INDEX_NAME}' in ChargebackFactTable.__table_args__, got: {index_names}"
        )

    def test_dimension_timestamp_index_column_order(self) -> None:
        """ix_chargeback_facts_dimension_timestamp must have dimension_id as leading column."""
        table_args = getattr(ChargebackFactTable, "__table_args__", ())
        target = next((arg for arg in table_args if isinstance(arg, Index) and arg.name == FACT_INDEX_NAME), None)
        assert target is not None, f"Index '{FACT_INDEX_NAME}' not found in ChargebackFactTable.__table_args__"

        col_names = [col.key if hasattr(col, "key") else str(col) for col in target.expressions]
        assert col_names[0] == "dimension_id", f"Leading column must be 'dimension_id', got: {col_names}"
        assert "timestamp" in col_names, f"'timestamp' must be in index columns, got: {col_names}"


class TestChargebackDimensionTableArgs:
    def test_table_args_has_eco_tenant_index(self) -> None:
        """ChargebackDimensionTable.__table_args__ must contain Index named ix_chargeback_dimensions_eco_tenant."""
        table_args = getattr(ChargebackDimensionTable, "__table_args__", None)
        assert table_args is not None, "ChargebackDimensionTable must have __table_args__"

        index_names = {arg.name for arg in table_args if isinstance(arg, Index)}
        assert DIM_INDEX_NAME in index_names, (
            f"Expected Index '{DIM_INDEX_NAME}' in ChargebackDimensionTable.__table_args__, got: {index_names}"
        )

    def test_eco_tenant_index_column_order(self) -> None:
        """ix_chargeback_dimensions_eco_tenant must have ecosystem as leading column."""
        table_args = getattr(ChargebackDimensionTable, "__table_args__", ())
        target = next((arg for arg in table_args if isinstance(arg, Index) and arg.name == DIM_INDEX_NAME), None)
        assert target is not None, f"Index '{DIM_INDEX_NAME}' not found in ChargebackDimensionTable.__table_args__"

        col_names = [col.key if hasattr(col, "key") else str(col) for col in target.expressions]
        assert col_names[0] == "ecosystem", f"Leading column must be 'ecosystem', got: {col_names}"
        assert "tenant_id" in col_names, f"'tenant_id' must be in index columns, got: {col_names}"


class TestChargebackIndexesIntegration:
    @pytest.fixture
    def engine(self):
        eng = create_engine("sqlite:///:memory:", echo=False)
        SQLModel.metadata.create_all(eng)
        yield eng
        eng.dispose()

    def test_create_all_creates_fact_dimension_timestamp_index(self, engine) -> None:
        """create_all must produce ix_chargeback_facts_dimension_timestamp on chargeback_facts."""
        inspector = sa_inspect(engine)
        indexes = {idx["name"] for idx in inspector.get_indexes("chargeback_facts")}
        assert FACT_INDEX_NAME in indexes, (
            f"Expected '{FACT_INDEX_NAME}' in chargeback_facts indexes after create_all, got: {indexes}"
        )

    def test_create_all_fact_index_has_correct_columns(self, engine) -> None:
        """ix_chargeback_facts_dimension_timestamp must cover (dimension_id, timestamp) in that order."""
        inspector = sa_inspect(engine)
        target = next(
            (idx for idx in inspector.get_indexes("chargeback_facts") if idx["name"] == FACT_INDEX_NAME),
            None,
        )
        assert target is not None, f"Index '{FACT_INDEX_NAME}' not found on chargeback_facts"
        cols = target["column_names"]
        assert cols[0] == "dimension_id", f"Leading column must be 'dimension_id', got: {cols}"
        assert "timestamp" in cols, f"'timestamp' must be in index columns, got: {cols}"

    def test_create_all_creates_dim_eco_tenant_index(self, engine) -> None:
        """create_all must produce ix_chargeback_dimensions_eco_tenant on chargeback_dimensions."""
        inspector = sa_inspect(engine)
        indexes = {idx["name"] for idx in inspector.get_indexes("chargeback_dimensions")}
        assert DIM_INDEX_NAME in indexes, (
            f"Expected '{DIM_INDEX_NAME}' in chargeback_dimensions indexes after create_all, got: {indexes}"
        )

    def test_create_all_dim_index_has_correct_columns(self, engine) -> None:
        """ix_chargeback_dimensions_eco_tenant must cover (ecosystem, tenant_id) in that order."""
        inspector = sa_inspect(engine)
        target = next(
            (idx for idx in inspector.get_indexes("chargeback_dimensions") if idx["name"] == DIM_INDEX_NAME),
            None,
        )
        assert target is not None, f"Index '{DIM_INDEX_NAME}' not found on chargeback_dimensions"
        cols = target["column_names"]
        assert cols[0] == "ecosystem", f"Leading column must be 'ecosystem', got: {cols}"
        assert "tenant_id" in cols, f"'tenant_id' must be in index columns, got: {cols}"


class TestMigration008:
    def test_migration_008_upgrade_creates_fact_index(self, tmp_path: pathlib.Path) -> None:
        """Migration 008 upgrade must create ix_chargeback_facts_dimension_timestamp."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "008")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        indexes = {idx["name"] for idx in inspector.get_indexes("chargeback_facts")}
        engine.dispose()

        assert FACT_INDEX_NAME in indexes, f"Expected '{FACT_INDEX_NAME}' after migration 008 upgrade, got: {indexes}"

    def test_migration_008_upgrade_creates_dim_index(self, tmp_path: pathlib.Path) -> None:
        """Migration 008 upgrade must create ix_chargeback_dimensions_eco_tenant."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "008")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        indexes = {idx["name"] for idx in inspector.get_indexes("chargeback_dimensions")}
        engine.dispose()

        assert DIM_INDEX_NAME in indexes, f"Expected '{DIM_INDEX_NAME}' after migration 008 upgrade, got: {indexes}"

    def test_migration_008_downgrade_drops_fact_index(self, tmp_path: pathlib.Path) -> None:
        """Migration 008 downgrade must remove ix_chargeback_facts_dimension_timestamp."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "008")
        command.downgrade(cfg, "007")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        indexes = {idx["name"] for idx in inspector.get_indexes("chargeback_facts")}
        engine.dispose()

        assert FACT_INDEX_NAME not in indexes, (
            f"'{FACT_INDEX_NAME}' must be absent after migration 008 downgrade, got: {indexes}"
        )

    def test_migration_008_downgrade_drops_dim_index(self, tmp_path: pathlib.Path) -> None:
        """Migration 008 downgrade must remove ix_chargeback_dimensions_eco_tenant."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "008")
        command.downgrade(cfg, "007")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        indexes = {idx["name"] for idx in inspector.get_indexes("chargeback_dimensions")}
        engine.dispose()

        assert DIM_INDEX_NAME not in indexes, (
            f"'{DIM_INDEX_NAME}' must be absent after migration 008 downgrade, got: {indexes}"
        )
