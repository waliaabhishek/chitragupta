from __future__ import annotations

import logging
import pathlib
from collections.abc import Generator
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy import inspect as sa_inspect
from sqlmodel import Session, SQLModel

from core.models.topic_attribution import TopicAttributionRow
from core.storage.backends.sqlmodel.engine import _engine_lock, _engines
from core.storage.backends.sqlmodel.repositories import TopicAttributionRepository

# ---------------------------------------------------------------------------
# Fixtures (mirrors test_migrations.py — no shared conftest in this dir)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_alembic_logging() -> Generator[None]:
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_level = root.level
    storage_logger = logging.getLogger("core.storage.backends.sqlmodel.repositories")
    original_disabled = storage_logger.disabled
    yield
    root.handlers = original_handlers
    root.setLevel(original_level)
    storage_logger.disabled = original_disabled
    for name in ["alembic", "alembic.runtime.migration"]:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(logging.NOTSET)


@pytest.fixture(autouse=True)
def clean_engine_cache() -> Generator[None]:
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()
    yield
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()


@pytest.fixture
def session() -> Generator[Session]:
    engine = create_engine("sqlite://", echo=False)
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s
    engine.dispose(close=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_alembic_cfg(conn: str):
    from alembic.config import Config

    migrations_dir = pathlib.Path(__file__).resolve().parents[4] / "src" / "core" / "storage" / "migrations"
    alembic_ini = migrations_dir / "alembic.ini"
    cfg = Config(str(alembic_ini))
    cfg.set_main_option("script_location", str(migrations_dir))
    cfg.set_main_option("sqlalchemy.url", conn)
    return cfg


def _make_row(
    topic_name: str = "orders",
    cluster_resource_id: str = "lkc-abc",
    ecosystem: str = "eco",
    tenant_id: str = "t1",
    env_id: str = "env-1",
    amount: Decimal = Decimal("10.00"),
    timestamp: datetime | None = None,
) -> TopicAttributionRow:
    return TopicAttributionRow(
        ecosystem=ecosystem,
        tenant_id=tenant_id,
        timestamp=timestamp or datetime(2026, 1, 1, tzinfo=UTC),
        env_id=env_id,
        cluster_resource_id=cluster_resource_id,
        topic_name=topic_name,
        product_category="KAFKA",
        product_type="KAFKA_NETWORK_WRITE",
        attribution_method="bytes_ratio",
        amount=amount,
    )


# ---------------------------------------------------------------------------
# Migration 017: schema changes
# ---------------------------------------------------------------------------


class TestMigration017SchemaChanges:
    def test_migration_017_upgrade_adds_resource_id_column(self, tmp_path) -> None:
        """Migration 017 upgrade adds resource_id column to topic_attribution_dimensions."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "016")
        command.upgrade(cfg, "017")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        columns = {c["name"] for c in inspector.get_columns("topic_attribution_dimensions")}
        engine.dispose()

        assert "resource_id" in columns

    def test_migration_017_upgrade_creates_index(self, tmp_path) -> None:
        """Migration 017 upgrade creates ix_topic_attr_dim_resource_id index."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "017")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(
                text("SELECT name FROM sqlite_master WHERE type='index' AND name='ix_topic_attr_dim_resource_id'")
            ).fetchall()
        engine.dispose()

        assert rows, "Index ix_topic_attr_dim_resource_id must exist after migration 017 upgrade"

    def test_migration_017_downgrade_removes_resource_id_column(self, tmp_path) -> None:
        """Migration 017 downgrade removes resource_id column from topic_attribution_dimensions."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "017")
        command.downgrade(cfg, "016")

        engine = create_engine(conn)
        inspector = sa_inspect(engine)
        columns = {c["name"] for c in inspector.get_columns("topic_attribution_dimensions")}
        engine.dispose()

        assert "resource_id" not in columns

    def test_migration_017_downgrade_removes_index(self, tmp_path) -> None:
        """Migration 017 downgrade removes ix_topic_attr_dim_resource_id index."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "017")
        command.downgrade(cfg, "016")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(
                text("SELECT name FROM sqlite_master WHERE type='index' AND name='ix_topic_attr_dim_resource_id'")
            ).fetchall()
        engine.dispose()

        assert not rows, "Index ix_topic_attr_dim_resource_id must be removed after downgrade"


# ---------------------------------------------------------------------------
# Migration 017: backfill correctness
# ---------------------------------------------------------------------------


class TestMigration017BackfillCorrectness:
    def test_migration_017_no_null_resource_ids_after_upgrade(self, tmp_path) -> None:
        """After migration 017, no rows in topic_attribution_dimensions have NULL resource_id."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "016")

        engine = create_engine(conn)
        with engine.connect() as c:
            c.execute(
                text("""
                INSERT INTO topic_attribution_dimensions
                    (ecosystem, tenant_id, env_id, cluster_resource_id, topic_name,
                     product_category, product_type, attribution_method)
                VALUES
                    ('eco', 't-1', 'env-1', 'lkc-abc', 'orders',
                     'KAFKA', 'KAFKA_NETWORK_WRITE', 'bytes_ratio'),
                    ('eco', 't-1', 'env-1', 'lkc-abc', 'payments',
                     'KAFKA', 'KAFKA_NETWORK_WRITE', 'bytes_ratio')
            """)
            )
            c.commit()
        engine.dispose()

        command.upgrade(cfg, "017")

        engine = create_engine(conn)
        with engine.connect() as c:
            null_count = c.execute(
                text("SELECT COUNT(*) FROM topic_attribution_dimensions WHERE resource_id IS NULL")
            ).scalar()
        engine.dispose()

        assert null_count == 0

    def test_migration_017_backfill_resource_id_equals_composed_key(self, tmp_path) -> None:
        """After migration 017, resource_id == cluster_resource_id || ':topic:' || topic_name for every row."""
        from alembic import command

        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        cfg = _get_alembic_cfg(conn)

        command.upgrade(cfg, "016")

        engine = create_engine(conn)
        with engine.connect() as c:
            c.execute(
                text("""
                INSERT INTO topic_attribution_dimensions
                    (ecosystem, tenant_id, env_id, cluster_resource_id, topic_name,
                     product_category, product_type, attribution_method)
                VALUES
                    ('eco', 't-1', 'env-1', 'lkc-0zgn12', 'dev.metrics.consumer.groups',
                     'KAFKA', 'KAFKA_NETWORK_WRITE', 'bytes_ratio'),
                    ('eco', 't-1', 'env-1', 'lkc-abc',    'orders',
                     'KAFKA', 'KAFKA_NETWORK_WRITE', 'bytes_ratio')
            """)
            )
            c.commit()
        engine.dispose()

        command.upgrade(cfg, "017")

        engine = create_engine(conn)
        with engine.connect() as c:
            rows = c.execute(
                text("SELECT cluster_resource_id, topic_name, resource_id FROM topic_attribution_dimensions")
            ).fetchall()
        engine.dispose()

        assert rows, "Expected rows after migration"
        for cluster_resource_id, topic_name, resource_id in rows:
            expected = f"{cluster_resource_id}:topic:{topic_name}"
            assert resource_id == expected, (
                f"resource_id mismatch for ({cluster_resource_id}, {topic_name}): "
                f"expected {expected!r}, got {resource_id!r}"
            )


# ---------------------------------------------------------------------------
# _get_or_create_dimension: resource_id populated on new rows
# ---------------------------------------------------------------------------


class TestGetOrCreateDimensionResourceId:
    def test_new_dimension_row_has_resource_id(self, session: Session) -> None:
        """_get_or_create_dimension sets resource_id = f'{cluster_resource_id}:topic:{topic_name}'."""
        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        repo = TopicAttributionRepository(session)
        repo.upsert_batch([_make_row(cluster_resource_id="lkc-abc", topic_name="orders")])
        session.commit()

        dim = session.exec(
            select(TopicAttributionDimensionTable).where(TopicAttributionDimensionTable.topic_name == "orders")
        ).one()
        assert dim.resource_id == "lkc-abc:topic:orders"

    def test_resource_id_format_with_dotted_topic_name(self, session: Session) -> None:
        """resource_id uses the exact composed format for dotted topic names."""
        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        repo = TopicAttributionRepository(session)
        repo.upsert_batch([_make_row(cluster_resource_id="lkc-0zgn12", topic_name="dev.metrics.consumer.groups")])
        session.commit()

        dim = session.exec(
            select(TopicAttributionDimensionTable).where(
                TopicAttributionDimensionTable.topic_name == "dev.metrics.consumer.groups"
            )
        ).one()
        assert dim.resource_id == "lkc-0zgn12:topic:dev.metrics.consumer.groups"

    def test_resource_id_not_empty_string(self, session: Session) -> None:
        """resource_id must not be empty string — implementation must set it explicitly."""
        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        repo = TopicAttributionRepository(session)
        repo.upsert_batch([_make_row(cluster_resource_id="lkc-xyz", topic_name="telemetry")])
        session.commit()

        dim = session.exec(
            select(TopicAttributionDimensionTable).where(TopicAttributionDimensionTable.topic_name == "telemetry")
        ).one()
        assert dim.resource_id != "", "resource_id must be populated, not empty string default"


# ---------------------------------------------------------------------------
# Integration: full data flow
# ---------------------------------------------------------------------------


class TestResourceIdIntegration:
    def test_upsert_then_read_dimension_has_correct_resource_id(self, session: Session) -> None:
        """Full flow: upsert TopicAttributionRow, read dimension back, verify resource_id."""
        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        repo = TopicAttributionRepository(session)
        repo.upsert_batch([_make_row(cluster_resource_id="lkc-xyz", topic_name="payments", amount=Decimal("42.00"))])
        session.commit()

        dim = session.exec(
            select(TopicAttributionDimensionTable).where(TopicAttributionDimensionTable.topic_name == "payments")
        ).one()
        assert dim.resource_id == "lkc-xyz:topic:payments"

    def test_multiple_topics_have_correct_resource_ids(self, session: Session) -> None:
        """Multiple distinct topics each get the correctly composed resource_id."""
        from sqlmodel import select

        from core.storage.backends.sqlmodel.tables import TopicAttributionDimensionTable

        repo = TopicAttributionRepository(session)
        repo.upsert_batch(
            [
                _make_row(cluster_resource_id="lkc-abc", topic_name="alpha"),
                _make_row(cluster_resource_id="lkc-abc", topic_name="beta"),
                _make_row(cluster_resource_id="lkc-xyz", topic_name="alpha"),
            ]
        )
        session.commit()

        dims = session.exec(select(TopicAttributionDimensionTable)).all()
        resource_ids = {(d.cluster_resource_id, d.topic_name): d.resource_id for d in dims}

        assert resource_ids[("lkc-abc", "alpha")] == "lkc-abc:topic:alpha"
        assert resource_ids[("lkc-abc", "beta")] == "lkc-abc:topic:beta"
        assert resource_ids[("lkc-xyz", "alpha")] == "lkc-xyz:topic:alpha"
