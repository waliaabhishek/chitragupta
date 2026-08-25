"""Production storage wiring tests for self-managed scope state."""

from __future__ import annotations

from pathlib import Path


def test_self_managed_storage_module_attaches_scope_repository_to_normal_unit_of_work(tmp_path: Path) -> None:
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
    from plugins.self_managed_kafka.storage.module import SelfManagedKafkaStorageModule

    backend = SQLModelBackend(
        f"sqlite:///{tmp_path / 'normal-runtime.db'}",
        SelfManagedKafkaStorageModule(),
        use_migrations=False,
    )
    backend.create_tables()
    try:
        with backend.create_unit_of_work() as uow:
            repository = uow.self_managed_kafka_scope_state
            assert repository.get("self_managed_kafka", "tenant-1", "billing-cluster-a") is None
    finally:
        backend.dispose()
