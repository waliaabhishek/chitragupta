from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlmodel import Session, col, create_engine, select

from core.models.chargeback import ChargebackRow, CostType
from core.storage.backends.sqlmodel.engine import _engine_lock, _engines
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.repositories import SQLModelChargebackRepository
from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

_NOW = datetime(2026, 2, 15, 0, tzinfo=UTC)


def _make_row(
    resource_id: str = "res-001",
    env_id: str | None = None,
    ecosystem: str = "confluent_cloud",
    identity_id: str = "user-1",
    cost_type: CostType = CostType.USAGE,
    amount: Decimal = Decimal("10.00"),
) -> ChargebackRow:
    metadata: dict[str, Any] = {}
    if env_id is not None:
        metadata["env_id"] = env_id
    return ChargebackRow(
        ecosystem=ecosystem,
        tenant_id="t-test",
        timestamp=_NOW,
        resource_id=resource_id,
        product_category="kafka",
        product_type="kafka_num_ckus",
        identity_id=identity_id,
        cost_type=cost_type,
        amount=amount,
        allocation_method="direct",
        allocation_detail=None,
        tags=[],
        metadata=metadata,
    )


@pytest.fixture(autouse=True)
def clean_engine_cache() -> Any:
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()
    yield
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()


class TestCCloudChargebackRepositoryEnvId:
    """Verification items 1-3: CCloudChargebackRepository stores and scopes env_id."""

    def test_upsert_ccloud_row_with_env_id_stores_env_id_on_dimension(self, tmp_path: Any) -> None:
        """Item 1: upsert(row_with_metadata_env_id) stores env_id on the dimension row."""
        from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable
        from plugins.confluent_cloud.storage.module import CCloudStorageModule

        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CCloudStorageModule(), use_migrations=False)
        backend.create_tables()

        row = _make_row(resource_id="res-env", env_id="env-alpha")
        with backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(row)
            uow.commit()

        engine = create_engine(conn)
        with Session(engine) as session:
            stmt = select(ChargebackDimensionTable).where(col(ChargebackDimensionTable.resource_id) == "res-env")
            dim = session.exec(stmt).first()
            assert dim is not None
            assert dim.env_id == "env-alpha"
        engine.dispose()
        backend.dispose()

    def test_upsert_non_ccloud_row_without_env_id_stores_empty_string(self, tmp_path: Any) -> None:
        """Item 2: upsert(row_without_metadata_env_id) stores env_id="" on the dimension row."""
        from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable

        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        row = _make_row(resource_id="res-noenv", env_id=None, ecosystem="self_managed_kafka")
        with backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(row)
            uow.commit()

        engine = create_engine(conn)
        with Session(engine) as session:
            stmt = select(ChargebackDimensionTable).where(col(ChargebackDimensionTable.resource_id) == "res-noenv")
            dim = session.exec(stmt).first()
            assert dim is not None
            assert dim.env_id == ""
        engine.dispose()
        backend.dispose()

    def test_two_rows_same_dims_different_env_id_produce_two_dimension_rows(self, tmp_path: Any) -> None:
        """Item 3: identical dimension fields, different env_id → two separate dimension rows (distinct UQ key)."""
        from core.storage.backends.sqlmodel.tables import ChargebackDimensionTable
        from plugins.confluent_cloud.storage.module import CCloudStorageModule

        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CCloudStorageModule(), use_migrations=False)
        backend.create_tables()

        # Same identity_id, cost_type, allocation_method — only env_id differs
        row_a = _make_row(resource_id="res-shared", env_id="env-alpha")
        row_b = _make_row(resource_id="res-shared", env_id="env-beta")
        with backend.create_unit_of_work() as uow:
            uow.chargebacks.upsert(row_a)
            uow.chargebacks.upsert(row_b)
            uow.commit()

        engine = create_engine(conn)
        with Session(engine) as session:
            stmt = select(ChargebackDimensionTable).where(col(ChargebackDimensionTable.resource_id) == "res-shared")
            dims = session.exec(stmt).all()
            assert len(dims) == 2
            env_ids = {d.env_id for d in dims}
            assert "env-alpha" in env_ids
            assert "env-beta" in env_ids
        engine.dispose()
        backend.dispose()


class TestStorageModuleCreateChargebackRepository:
    """Verification items 9-10: module factory returns correct concrete type."""

    def test_ccloud_storage_module_create_chargeback_repository_returns_ccloud_repo(self, tmp_path: Any) -> None:
        """Item 9: CCloudStorageModule().create_chargeback_repository(session) → CCloudChargebackRepository."""
        from plugins.confluent_cloud.storage.module import CCloudStorageModule
        from plugins.confluent_cloud.storage.repositories import CCloudChargebackRepository

        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CCloudStorageModule(), use_migrations=False)
        backend.create_tables()

        engine = create_engine(conn)
        with Session(engine) as session:
            module = CCloudStorageModule()
            repo = module.create_chargeback_repository(session)
            assert isinstance(repo, CCloudChargebackRepository)
        engine.dispose()
        backend.dispose()

    def test_core_storage_module_create_chargeback_repository_returns_sqlmodel_repo(self, tmp_path: Any) -> None:
        """Item 10: CoreStorageModule().create_chargeback_repository(session) → SQLModelChargebackRepository."""
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        engine = create_engine(conn)
        with Session(engine) as session:
            module = CoreStorageModule()
            repo = module.create_chargeback_repository(session)
            assert isinstance(repo, SQLModelChargebackRepository)
        engine.dispose()
        backend.dispose()


class TestSQLModelUoWWithCCloudModule:
    """Verification item 11: UoW with CCloudStorageModule exposes CCloudChargebackRepository."""

    def test_sqlmodel_uow_with_ccloud_module_has_ccloud_chargeback_repository(self, tmp_path: Any) -> None:
        """Item 11: uow.chargebacks is CCloudChargebackRepository when built with CCloudStorageModule."""
        from plugins.confluent_cloud.storage.module import CCloudStorageModule
        from plugins.confluent_cloud.storage.repositories import CCloudChargebackRepository

        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CCloudStorageModule(), use_migrations=False)
        backend.create_tables()

        with backend.create_unit_of_work() as uow:
            assert isinstance(uow.chargebacks, CCloudChargebackRepository)
        backend.dispose()


class TestEnvIdColumnExistsOnDimensionTable:
    """Verification item 12: env_id column is queryable directly on chargeback_dimensions."""

    def test_env_id_column_exists_on_chargeback_dimensions_table(self, tmp_path: Any) -> None:
        """Item 12: chargeback_dimensions has env_id column — Grafana can query d.env_id without a join."""
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        columns = {c["name"] for c in inspector.get_columns("chargeback_dimensions")}
        assert "env_id" in columns
        engine.dispose()
        backend.dispose()

    def test_env_id_column_is_in_unique_constraint(self, tmp_path: Any) -> None:
        """env_id is part of the unique constraint (10-field key) on chargeback_dimensions."""
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        engine = create_engine(conn)
        constraints = sa_inspect(engine).get_unique_constraints("chargeback_dimensions")
        engine.dispose()
        backend.dispose()

        uq = next((c for c in constraints if c["name"] == "uq_chargeback_dimensions"), None)
        assert uq is not None, "uq_chargeback_dimensions constraint not found"
        assert "env_id" in uq["column_names"]


class TestGetDimensionEnvId:
    """Gap 3: get_dimension() and get_dimensions_batch() must expose env_id via ChargebackDimensionInfo."""

    def test_get_dimension_returns_env_id(self, tmp_path: Any) -> None:
        """Item 5: repo.get_dimension(id) on a row with env_id='env-123' → result.env_id == 'env-123'."""
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        engine = create_engine(conn)
        with Session(engine) as session:
            dim = ChargebackDimensionTable(
                ecosystem="confluent_cloud",
                tenant_id="t-1",
                resource_id="cluster-1",
                product_category="kafka",
                product_type="kafka_num_ckus",
                identity_id="user-1",
                cost_type="usage",
                allocation_method="direct",
                allocation_detail=None,
                env_id="env-123",
            )
            session.add(dim)
            session.flush()
            dimension_id = dim.dimension_id

            repo = SQLModelChargebackRepository(session)
            result = repo.get_dimension(dimension_id)

        engine.dispose()
        backend.dispose()

        assert result is not None
        assert result.env_id == "env-123"

    def test_get_dimensions_batch_returns_env_id_for_all_rows(self, tmp_path: Any) -> None:
        """Item 6: get_dimensions_batch([id1, id2]) returns correct env_id for each row."""
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        engine = create_engine(conn)
        with Session(engine) as session:
            dim_a = ChargebackDimensionTable(
                ecosystem="confluent_cloud",
                tenant_id="t-1",
                resource_id="cluster-a",
                product_category="kafka",
                product_type="kafka_num_ckus",
                identity_id="user-a",
                cost_type="usage",
                allocation_method="direct",
                allocation_detail=None,
                env_id="env-aaa",
            )
            dim_b = ChargebackDimensionTable(
                ecosystem="confluent_cloud",
                tenant_id="t-1",
                resource_id="cluster-b",
                product_category="kafka",
                product_type="kafka_num_ckus",
                identity_id="user-b",
                cost_type="shared",
                allocation_method="even",
                allocation_detail=None,
                env_id="env-bbb",
            )
            session.add(dim_a)
            session.add(dim_b)
            session.flush()
            id_a = dim_a.dimension_id
            id_b = dim_b.dimension_id

            repo = SQLModelChargebackRepository(session)
            result = repo.get_dimensions_batch([id_a, id_b])

        engine.dispose()
        backend.dispose()

        assert id_a in result
        assert id_b in result
        assert result[id_a].env_id == "env-aaa"
        assert result[id_b].env_id == "env-bbb"
