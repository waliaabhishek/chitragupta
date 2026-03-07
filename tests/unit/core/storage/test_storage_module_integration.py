from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from core.storage.backends.sqlmodel.engine import _engine_lock, _engines
from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend, SQLModelUnitOfWork
from core.storage.interface import UnitOfWork


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


def _make_mock_storage_module() -> MagicMock:
    """Return a mock that satisfies the StorageModule protocol."""
    module = MagicMock()
    # create_billing_repository, create_resource_repository, create_identity_repository
    # and register_tables must be present — MagicMock provides them automatically.
    return module


class TestSQLModelUnitOfWorkRequiresStorageModule:
    def test_missing_storage_module_raises_type_error(self, tmp_path) -> None:
        """SQLModelUnitOfWork(connection_string) without storage_module must raise TypeError.

        After implementation, the constructor signature is:
            __init__(self, connection_string: str, storage_module: StorageModule)
        Calling it with only one positional arg must fail at construction time.
        """
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        with pytest.raises(TypeError):
            SQLModelUnitOfWork(conn)  # type: ignore[call-arg]

    def test_with_storage_module_does_not_raise(self, tmp_path) -> None:
        """SQLModelUnitOfWork constructed with both args succeeds."""
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        storage_module = _make_mock_storage_module()
        # Should not raise
        uow = SQLModelUnitOfWork(conn, storage_module)
        assert uow is not None


class TestSQLModelBackendRequiresStorageModule:
    def test_missing_storage_module_raises_type_error(self, tmp_path) -> None:
        """SQLModelBackend(connection_string) without storage_module must raise TypeError.

        After implementation, the constructor signature is:
            __init__(self, connection_string: str, storage_module: StorageModule, *, use_migrations: bool)
        """
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        with pytest.raises(TypeError):
            SQLModelBackend(conn)  # type: ignore[call-arg]

    def test_create_unit_of_work_takes_no_args(self, tmp_path) -> None:
        """SQLModelBackend.create_unit_of_work() must take no args (StorageBackend protocol compliance).

        StorageModule is injected at SQLModelBackend construction time — not passed to create_unit_of_work().
        """
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        storage_module = _make_mock_storage_module()
        backend = SQLModelBackend(conn, storage_module, use_migrations=False)
        # Must succeed with no args
        uow = backend.create_unit_of_work()
        assert uow is not None

    def test_create_unit_of_work_returns_unit_of_work_protocol(self, tmp_path) -> None:
        """create_unit_of_work() returns an object satisfying UnitOfWork protocol."""
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        storage_module = _make_mock_storage_module()
        backend = SQLModelBackend(conn, storage_module, use_migrations=False)
        uow = backend.create_unit_of_work()
        assert isinstance(uow, UnitOfWork)


class TestSQLModelBackendStorageModuleWiring:
    def test_storage_module_create_billing_repository_called_on_enter(self, tmp_path) -> None:
        """On UoW __enter__, storage_module.create_billing_repository(session) is called."""
        conn = f"sqlite:///{tmp_path / 'test.db'}"
        storage_module = _make_mock_storage_module()

        # Need actual tables for core repos; plugin tables handled by register_tables
        from sqlmodel import SQLModel

        engine_module = __import__("core.storage.backends.sqlmodel.engine", fromlist=["get_or_create_engine"])
        engine = engine_module.get_or_create_engine(conn)
        # Create core tables only (chargeback, pipeline_state, etc.) for context manager to work
        from core.storage.backends.sqlmodel import tables  # noqa: F401

        SQLModel.metadata.create_all(engine)

        backend = SQLModelBackend(conn, storage_module, use_migrations=False)
        with backend.create_unit_of_work():
            storage_module.create_billing_repository.assert_called_once()
            storage_module.create_resource_repository.assert_called_once()
            storage_module.create_identity_repository.assert_called_once()
