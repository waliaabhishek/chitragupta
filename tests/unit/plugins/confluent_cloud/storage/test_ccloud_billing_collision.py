from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from core.storage.backends.sqlmodel.engine import _engine_lock, _engines

# After implementation: these modules will exist.
# Currently they do not — ImportError puts tests in red state.
from plugins.confluent_cloud.models.billing import CCloudBillingLineItem
from plugins.confluent_cloud.storage.module import CCloudStorageModule

_NOW = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def clean_engine_cache():
    """Clean engine cache before/after each test."""
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()
    yield
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()


def _make_ccloud_line(env_id: str, resource_id: str = "lkc-xxxxx") -> CCloudBillingLineItem:
    return CCloudBillingLineItem(
        ecosystem="confluent_cloud",
        tenant_id="org-001",
        timestamp=_NOW,
        env_id=env_id,
        resource_id=resource_id,
        product_category="kafka",
        product_type="kafka_num_ckus",
        quantity=Decimal("10"),
        unit_price=Decimal("0.10"),
        total_cost=Decimal("1.00"),
        currency="USD",
        granularity="hourly",
    )


@pytest.fixture
def ccloud_backend(tmp_path):
    """SQLModelBackend wired with CCloudStorageModule, tables created."""
    from sqlmodel import SQLModel

    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

    conn = f"sqlite:///{tmp_path / 'test.db'}"
    storage_module = CCloudStorageModule()
    backend = SQLModelBackend(conn, storage_module, use_migrations=False)

    # Create core + plugin tables
    import plugins.confluent_cloud.storage.tables  # noqa: F401
    from core.storage.backends.sqlmodel import tables  # noqa: F401
    from core.storage.backends.sqlmodel.engine import get_or_create_engine

    engine = get_or_create_engine(conn)
    SQLModel.metadata.create_all(engine)
    storage_module.register_tables(engine)

    yield backend
    backend.dispose()


class TestCCloudBillingEnvIdCollision:
    def test_two_env_billing_lines_both_persist(self, ccloud_backend) -> None:
        """Two CCloud lines differing only in env_id must both persist — no overwrite.

        Before fix: both lines share the same PK (env_id absent), second overwrites first.
        After fix: 7-field PK includes env_id, both rows survive.
        """
        line_env_aaa = _make_ccloud_line(env_id="env-aaa")
        line_env_bbb = _make_ccloud_line(env_id="env-bbb")

        with ccloud_backend.create_unit_of_work() as uow:
            uow.billing.upsert(line_env_aaa)
            uow.billing.upsert(line_env_bbb)
            uow.commit()

        with ccloud_backend.create_unit_of_work() as uow:
            rows = uow.billing.find_by_date("confluent_cloud", "org-001", _NOW.date())

        assert len(rows) == 2, (
            f"Expected 2 billing rows (one per env_id), got {len(rows)}. "
            "env_id is missing from the billing table PK — rows overwrite each other."
        )
        env_ids = {r.env_id for r in rows}  # type: ignore[attr-defined]
        assert env_ids == {"env-aaa", "env-bbb"}

    def test_same_env_same_resource_upserts(self, ccloud_backend) -> None:
        """Two upserts with identical PK (same env_id) produce one row — idempotent."""
        line = _make_ccloud_line(env_id="env-aaa")

        with ccloud_backend.create_unit_of_work() as uow:
            uow.billing.upsert(line)
            uow.billing.upsert(line)
            uow.commit()

        with ccloud_backend.create_unit_of_work() as uow:
            rows = uow.billing.find_by_date("confluent_cloud", "org-001", _NOW.date())

        assert len(rows) == 1

    def test_different_resources_same_env_both_persist(self, ccloud_backend) -> None:
        """Different resource_ids in the same env both persist independently."""
        line_cluster_a = _make_ccloud_line(env_id="env-aaa", resource_id="lkc-aaaaa")
        line_cluster_b = _make_ccloud_line(env_id="env-aaa", resource_id="lkc-bbbbb")

        with ccloud_backend.create_unit_of_work() as uow:
            uow.billing.upsert(line_cluster_a)
            uow.billing.upsert(line_cluster_b)
            uow.commit()

        with ccloud_backend.create_unit_of_work() as uow:
            rows = uow.billing.find_by_date("confluent_cloud", "org-001", _NOW.date())

        assert len(rows) == 2

    def test_ccloud_billing_table_has_env_id_column(self, tmp_path) -> None:
        """CCloud billing table schema must include env_id column."""
        from sqlmodel import SQLModel

        import plugins.confluent_cloud.storage.tables as ccloud_tables  # noqa: F401
        from core.storage.backends.sqlmodel.engine import get_or_create_engine
        from plugins.confluent_cloud.storage.tables import CCloudBillingTable

        conn = f"sqlite:///{tmp_path / 'schema_check.db'}"
        engine = get_or_create_engine(conn)
        SQLModel.metadata.create_all(engine)

        columns = {col.name for col in CCloudBillingTable.__table__.columns}
        assert "env_id" in columns, "CCloudBillingTable missing env_id column"

    def test_ccloud_billing_table_pk_includes_env_id(self) -> None:
        """CCloud billing table PK must include env_id — 7-field composite key."""
        import plugins.confluent_cloud.storage.tables  # noqa: F401
        from plugins.confluent_cloud.storage.tables import CCloudBillingTable

        pk_cols = {col.name for col in CCloudBillingTable.__table__.primary_key.columns}
        expected_pk = {
            "ecosystem",
            "tenant_id",
            "timestamp",
            "env_id",
            "resource_id",
            "product_type",
            "product_category",
        }
        assert pk_cols == expected_pk, (
            f"CCloudBillingTable PK mismatch.\n  Expected: {expected_pk}\n  Got:      {pk_cols}"
        )


class TestCCloudBillingRepositoryOperations:
    """Coverage for CCloudBillingRepository methods beyond basic upsert."""

    def test_delete_before_removes_old_rows(self, ccloud_backend) -> None:
        """delete_before() removes rows older than the cutoff datetime."""
        old_line = _make_ccloud_line(env_id="env-aaa")
        old_line = CCloudBillingLineItem(**{**old_line.__dict__, "timestamp": datetime(2024, 1, 1, tzinfo=UTC)})
        new_line = _make_ccloud_line(env_id="env-aaa")

        with ccloud_backend.create_unit_of_work() as uow:
            uow.billing.upsert(old_line)
            uow.billing.upsert(new_line)
            uow.commit()

        cutoff = datetime(2024, 1, 10, tzinfo=UTC)
        with ccloud_backend.create_unit_of_work() as uow:
            deleted = uow.billing.delete_before("confluent_cloud", "org-001", cutoff)
            uow.commit()

        assert deleted == 1

        with ccloud_backend.create_unit_of_work() as uow:
            rows = uow.billing.find_by_date("confluent_cloud", "org-001", _NOW.date())
        assert len(rows) == 1

    def test_find_by_filters_returns_matching_rows(self, ccloud_backend) -> None:
        """find_by_filters() returns (items, total) filtered by product_type."""
        line_kafka = _make_ccloud_line(env_id="env-aaa")
        line_sr = CCloudBillingLineItem(
            **{**line_kafka.__dict__, "env_id": "env-bbb", "product_type": "schema_registry"}
        )

        with ccloud_backend.create_unit_of_work() as uow:
            uow.billing.upsert(line_kafka)
            uow.billing.upsert(line_sr)
            uow.commit()

        with ccloud_backend.create_unit_of_work() as uow:
            items, total = uow.billing.find_by_filters("confluent_cloud", "org-001", product_type="kafka_num_ckus")

        assert total == 1
        assert len(items) == 1
        assert items[0].product_type == "kafka_num_ckus"

    def test_increment_allocation_attempts_raises_key_error_for_missing(self, ccloud_backend) -> None:
        """increment_allocation_attempts() raises KeyError when row not found."""
        missing_line = _make_ccloud_line(env_id="env-missing")

        with ccloud_backend.create_unit_of_work() as uow, pytest.raises(KeyError):
            uow.billing.increment_allocation_attempts(missing_line)

    def test_find_by_range_returns_matching_rows(self, ccloud_backend) -> None:
        """find_by_range() returns rows within the given datetime range."""
        line = _make_ccloud_line(env_id="env-aaa")

        with ccloud_backend.create_unit_of_work() as uow:
            uow.billing.upsert(line)
            uow.commit()

        start = datetime(2024, 1, 15, 0, 0, 0, tzinfo=UTC)
        end = datetime(2024, 1, 16, 0, 0, 0, tzinfo=UTC)
        with ccloud_backend.create_unit_of_work() as uow:
            rows = uow.billing.find_by_range("confluent_cloud", "org-001", start, end)

        assert len(rows) == 1
        assert rows[0].env_id == "env-aaa"

    def test_find_by_range_excludes_out_of_range(self, ccloud_backend) -> None:
        """find_by_range() excludes rows outside the given datetime range."""
        line = _make_ccloud_line(env_id="env-aaa")

        with ccloud_backend.create_unit_of_work() as uow:
            uow.billing.upsert(line)
            uow.commit()

        # Range that does NOT include _NOW (2024-01-15 12:00:00)
        start = datetime(2024, 1, 16, 0, 0, 0, tzinfo=UTC)
        end = datetime(2024, 1, 17, 0, 0, 0, tzinfo=UTC)
        with ccloud_backend.create_unit_of_work() as uow:
            rows = uow.billing.find_by_range("confluent_cloud", "org-001", start, end)

        assert len(rows) == 0

    def test_increment_allocation_attempts_success(self, ccloud_backend) -> None:
        """increment_allocation_attempts() returns updated count for existing row."""
        line = _make_ccloud_line(env_id="env-aaa")

        with ccloud_backend.create_unit_of_work() as uow:
            uow.billing.upsert(line)
            uow.commit()

        with ccloud_backend.create_unit_of_work() as uow:
            count = uow.billing.increment_allocation_attempts(line)
            uow.commit()

        assert count == 1


class TestCCloudStorageModuleProtocol:
    def test_ccloud_storage_module_satisfies_storage_module_protocol(self) -> None:
        """CCloudStorageModule must satisfy the StorageModule Protocol."""
        from core.plugin.protocols import StorageModule

        module = CCloudStorageModule()
        assert isinstance(module, StorageModule)

    def test_ccloud_storage_module_create_billing_repository(self, tmp_path) -> None:
        """create_billing_repository() returns a BillingRepository."""
        from unittest.mock import MagicMock

        from core.storage.interface import BillingRepository

        module = CCloudStorageModule()
        session = MagicMock()
        repo = module.create_billing_repository(session)
        assert isinstance(repo, BillingRepository)
