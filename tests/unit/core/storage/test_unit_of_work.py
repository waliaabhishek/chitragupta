from __future__ import annotations

from datetime import UTC, datetime

import pytest

from core.models.resource import CoreResource, ResourceStatus
from core.storage.backends.sqlmodel.engine import _engine_lock, _engines
from core.storage.backends.sqlmodel.module import CoreStorageModule
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend, SQLModelUnitOfWork


@pytest.fixture(autouse=True)
def clean_engine_cache():
    """Clean engine cache before each test to avoid cross-test contamination."""
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()
    yield
    with _engine_lock:
        for e in _engines.values():
            e.dispose()
        _engines.clear()


class TestSQLModelUnitOfWork:
    def test_commit_persists(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        r = CoreResource(
            ecosystem="eco",
            tenant_id="t1",
            resource_id="r1",
            resource_type="kafka",
            status=ResourceStatus.ACTIVE,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        with backend.create_unit_of_work() as uow:
            uow.resources.upsert(r)
            uow.commit()

        # Read in new UoW — should persist
        with backend.create_unit_of_work() as uow:
            got = uow.resources.get("eco", "t1", "r1")
            assert got is not None
            assert got.resource_type == "kafka"

        backend.dispose()

    def test_rollback_discards(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        with backend.create_unit_of_work() as uow:
            r = CoreResource(
                ecosystem="eco",
                tenant_id="t1",
                resource_id="r1",
                resource_type="kafka",
                status=ResourceStatus.ACTIVE,
            )
            uow.resources.upsert(r)
            uow.rollback()

        with backend.create_unit_of_work() as uow:
            got = uow.resources.get("eco", "t1", "r1")
            assert got is None

        backend.dispose()

    def test_exception_triggers_rollback(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        with pytest.raises(ValueError, match="boom"), backend.create_unit_of_work() as uow:
            r = CoreResource(
                ecosystem="eco",
                tenant_id="t1",
                resource_id="r1",
                resource_type="kafka",
                status=ResourceStatus.ACTIVE,
            )
            uow.resources.upsert(r)
            raise ValueError("boom")

        with backend.create_unit_of_work() as uow:
            got = uow.resources.get("eco", "t1", "r1")
            assert got is None

        backend.dispose()

    def test_repo_attributes_accessible(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        with backend.create_unit_of_work() as uow:
            assert uow.resources is not None
            assert uow.identities is not None
            assert uow.billing is not None
            assert uow.chargebacks is not None
            assert uow.pipeline_state is not None
            assert uow.tags is not None

        backend.dispose()

    def test_commit_outside_context_raises(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        uow = SQLModelUnitOfWork(conn, CoreStorageModule())
        with pytest.raises(RuntimeError, match="Cannot commit"):
            uow.commit()

    def test_no_commit_means_rollback(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        with backend.create_unit_of_work() as uow:
            r = CoreResource(
                ecosystem="eco",
                tenant_id="t1",
                resource_id="r1",
                resource_type="kafka",
                status=ResourceStatus.ACTIVE,
            )
            uow.resources.upsert(r)
            # No commit — should be rolled back on __exit__

        with backend.create_unit_of_work() as uow:
            got = uow.resources.get("eco", "t1", "r1")
            assert got is None

        backend.dispose()

    def test_rollback_outside_context_raises(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        uow = SQLModelUnitOfWork(conn, CoreStorageModule())
        with pytest.raises(RuntimeError, match="Cannot rollback"):
            uow.rollback()


class TestSQLModelBackend:
    def test_create_tables_without_migrations(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        # Verify tables exist by using a UoW
        with backend.create_unit_of_work() as uow:
            r = CoreResource(
                ecosystem="eco",
                tenant_id="t1",
                resource_id="r1",
                resource_type="kafka",
                status=ResourceStatus.ACTIVE,
            )
            uow.resources.upsert(r)
            uow.commit()

        backend.dispose()

    def test_dispose(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()
        backend.dispose()
        # After dispose, engine is cleaned up (no exception = pass)
