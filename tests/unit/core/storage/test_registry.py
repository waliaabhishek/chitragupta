from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from core.config.models import StorageConfig


class TestCreateStorageBackendFunction:
    def test_creates_sqlmodel_backend(self) -> None:
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
        from core.storage.registry import create_storage_backend

        config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")

        result = create_storage_backend(config, use_migrations=False)

        assert isinstance(result, SQLModelBackend)
        result.dispose()

    def test_passes_use_migrations_false(self) -> None:
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
        from core.storage.registry import create_storage_backend

        config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")

        result = create_storage_backend(config, use_migrations=False)

        assert isinstance(result, SQLModelBackend)
        result.dispose()

    def test_default_use_migrations_is_true(self) -> None:
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
        from core.storage.registry import create_storage_backend

        config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")

        # Patch the migration runner to avoid actually running migrations
        with patch.object(SQLModelBackend, "_run_migrations"):
            result = create_storage_backend(config)

        assert isinstance(result, SQLModelBackend)
        result.dispose()

    def test_unknown_backend_raises_value_error(self) -> None:
        from core.storage.registry import create_storage_backend

        config = StorageConfig(backend="unknown", connection_string="sqlite:///:memory:")

        with pytest.raises(ValueError, match="Unknown storage backend"):
            create_storage_backend(config)

    def test_accepts_storage_module_parameter(self) -> None:
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend
        from core.storage.registry import create_storage_backend
        from plugins.confluent_cloud.storage.module import CCloudStorageModule

        config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        storage_module = CCloudStorageModule()

        result = create_storage_backend(config, storage_module=storage_module, use_migrations=False)

        assert isinstance(result, SQLModelBackend)
        result.dispose()


class TestGetStorageModuleForEcosystem:
    """Tests for plugins.storage_modules.get_storage_module_for_ecosystem."""

    def test_confluent_cloud_returns_ccloud_module(self) -> None:
        from plugins.confluent_cloud.storage.module import CCloudStorageModule
        from plugins.storage_modules import get_storage_module_for_ecosystem

        result = get_storage_module_for_ecosystem("confluent_cloud")

        assert isinstance(result, CCloudStorageModule)

    def test_other_ecosystem_returns_core_module(self) -> None:
        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from plugins.storage_modules import get_storage_module_for_ecosystem

        result = get_storage_module_for_ecosystem("self_managed_kafka")

        assert isinstance(result, CoreStorageModule)

    def test_unknown_ecosystem_returns_core_module(self) -> None:
        from core.storage.backends.sqlmodel.module import CoreStorageModule
        from plugins.storage_modules import get_storage_module_for_ecosystem

        result = get_storage_module_for_ecosystem("some_random_ecosystem")

        assert isinstance(result, CoreStorageModule)


class TestWorkflowRunnerNoPrivateFunction:
    def test_create_storage_backend_removed_from_workflow_runner(self) -> None:
        import workflow_runner

        assert not hasattr(workflow_runner, "_create_storage_backend"), (
            "_create_storage_backend should not exist in workflow_runner after TASK-007"
        )


def test_get_storage_backend_leases_the_request_provider_for_the_dependency_lifetime() -> None:
    from core.api.dependencies import get_storage_backend
    from core.storage.backend_provider import TenantBackendProvider

    backend = MagicMock()
    lease = MagicMock()
    lease.__enter__.return_value = backend
    provider = MagicMock(spec=TenantBackendProvider)
    provider.acquire_backend.return_value = lease
    request = MagicMock()
    request.app.state.backend_provider = provider
    tenant_config = MagicMock()

    dependency = get_storage_backend(request, "tenant-a", tenant_config)
    assert next(dependency) is backend
    with pytest.raises(StopIteration):
        next(dependency)

    provider.acquire_backend.assert_called_once_with("tenant-a", tenant_config)
    lease.__exit__.assert_called_once()


def test_generic_read_only_uow_never_constructs_preview_repository(tmp_path: object) -> None:
    from pathlib import Path

    from core.storage.backends.sqlmodel.unit_of_work import ReadOnlySQLModelUnitOfWork, SQLModelBackend
    from plugins.confluent_cloud.storage.module import CCloudStorageModule

    database = Path(str(tmp_path)) / "generic-read-only.db"
    connection_string = f"sqlite:///{database}"
    module = CCloudStorageModule()
    backend = SQLModelBackend(
        connection_string,
        module,
        use_migrations=False,
        focus_preview_enabled=True,
    )
    backend.create_tables()

    try:
        with (
            patch.object(
                module,
                "create_preview_source_attempt_fallback_repository",
                side_effect=AssertionError("Preview repository constructed"),
            ) as preview_repository,
            ReadOnlySQLModelUnitOfWork(connection_string, module) as uow,
        ):
            assert uow.preview_evidence_enabled is False
            assert uow.resources is not None
        preview_repository.assert_not_called()
    finally:
        backend.dispose()
