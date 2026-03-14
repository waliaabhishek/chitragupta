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
        # Open a connection to put it in the pool
        with backend.create_unit_of_work() as uow:
            _ = uow.resources.get("eco", "t1", "r1")
        backend.dispose()
        # After dispose(), the pool has no checked-out connections
        assert backend._engine.pool.checkedout() == 0

    def test_dispose_closes_both_engine_pools(self, tmp_path: object) -> None:
        """SQLModelBackend.dispose() must dispose both write and read-only engines."""
        db_path = tmp_path / "test.db"  # type: ignore[operator]
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        # Open connections on both pools so they have something to close
        with backend.create_unit_of_work() as uow:
            _ = uow.resources.get("eco", "t1", "r1")
        with backend.create_read_only_unit_of_work() as ro_uow:
            _ = ro_uow.resources.get("eco", "t1", "r1")

        backend.dispose()

        # After dispose(), both pools report zero checked-out connections
        assert backend._engine.pool.checkedout() == 0  # type: ignore[attr-defined]
        assert backend._ro_engine.pool.checkedout() == 0  # type: ignore[attr-defined]

    def test_storage_backend_protocol_conformance_includes_read_only(self, tmp_path: object) -> None:
        """StorageBackend protocol must include create_read_only_unit_of_work."""
        from core.storage.backends.sqlmodel.unit_of_work import ReadOnlySQLModelUnitOfWork  # noqa: F401 — ImportError = red
        from core.storage.interface import StorageBackend

        db_path = tmp_path / "test.db"  # type: ignore[operator]
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        assert isinstance(backend, StorageBackend)
        # After protocol update, create_read_only_unit_of_work must be present
        assert hasattr(backend, "create_read_only_unit_of_work")


class TestReadOnlySQLModelUnitOfWork:
    def test_commit_raises_runtime_error(self, tmp_path: object) -> None:
        """ReadOnlySQLModelUnitOfWork.commit() must raise RuntimeError mentioning 'read-only'."""
        from core.storage.backends.sqlmodel.unit_of_work import ReadOnlySQLModelUnitOfWork  # ImportError = red

        db_path = tmp_path / "test.db"  # type: ignore[operator]
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        with pytest.raises(RuntimeError, match="read-only"):
            with backend.create_read_only_unit_of_work() as uow:
                assert isinstance(uow, ReadOnlySQLModelUnitOfWork)
                uow.commit()

        backend.dispose()

    def test_pragma_query_only_is_applied(self) -> None:
        """get_or_create_read_only_engine must set PRAGMA query_only=1 on SQLite connections."""
        from core.storage.backends.sqlmodel.engine import get_or_create_read_only_engine  # ImportError = red

        engine = get_or_create_read_only_engine("sqlite:///:memory:")
        with engine.connect() as conn:
            result = conn.exec_driver_sql("PRAGMA query_only").fetchone()
        assert result is not None
        assert result[0] == 1

    def test_read_operations_work(self, tmp_path: object) -> None:
        """Read-only UoW repositories are accessible and queries return without error."""
        from core.storage.backends.sqlmodel.unit_of_work import ReadOnlySQLModelUnitOfWork  # ImportError = red

        db_path = tmp_path / "test.db"  # type: ignore[operator]
        conn = f"sqlite:///{db_path}"
        backend = SQLModelBackend(conn, CoreStorageModule(), use_migrations=False)
        backend.create_tables()

        with backend.create_read_only_unit_of_work() as uow:
            assert isinstance(uow, ReadOnlySQLModelUnitOfWork)
            # Repositories are accessible
            assert uow.resources is not None
            assert uow.identities is not None
            assert uow.pipeline_state is not None
            # Query returns None without error (no data inserted)
            result = uow.resources.get("eco", "t1", "r1")
            assert result is None

        backend.dispose()

    def test_dispose_all_engines_clears_readonly_entry(self, tmp_path: object) -> None:
        """dispose_all_engines() must clear entries keyed with 'readonly:' prefix."""
        from core.storage.backends.sqlmodel.engine import (
            _engines,
            dispose_all_engines,
            get_or_create_engine,
            get_or_create_read_only_engine,  # ImportError = red
        )

        db_path = tmp_path / "test.db"  # type: ignore[operator]
        conn = f"sqlite:///{db_path}"

        get_or_create_engine(conn)
        get_or_create_read_only_engine(conn)

        dispose_all_engines()

        assert conn not in _engines
        assert f"readonly:{conn}" not in _engines
