from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from core.config.models import StorageConfig

if TYPE_CHECKING:
    from core.storage.interface import StorageBackend


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


class TestGetOrCreateBackendAcceptsStorageConfig:
    @patch("core.api.dependencies.create_storage_backend")
    @patch("core.api.dependencies.get_storage_module_for_ecosystem")
    def test_respects_storage_config_backend(self, mock_get_module: MagicMock, mock_create: MagicMock) -> None:
        from core.api.dependencies import get_or_create_backend

        mock_module = MagicMock()
        mock_get_module.return_value = mock_module
        mock_backend = MagicMock()
        mock_create.return_value = mock_backend

        storage_config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        backends: dict[str, StorageBackend] = {}

        result = get_or_create_backend(backends, "tenant-a", storage_config, "confluent_cloud")

        mock_get_module.assert_called_once_with("confluent_cloud")
        mock_create.assert_called_once_with(storage_config, storage_module=mock_module, use_migrations=False)
        assert result is mock_backend

    @patch("core.api.dependencies.create_storage_backend")
    @patch("core.api.dependencies.get_storage_module_for_ecosystem")
    def test_caches_backend_per_tenant(self, mock_get_module: MagicMock, mock_create: MagicMock) -> None:
        from core.api.dependencies import get_or_create_backend

        mock_module = MagicMock()
        mock_get_module.return_value = mock_module
        mock_backend = MagicMock()
        mock_create.return_value = mock_backend

        storage_config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        backends: dict[str, StorageBackend] = {}

        result1 = get_or_create_backend(backends, "tenant-a", storage_config, "confluent_cloud")
        result2 = get_or_create_backend(backends, "tenant-a", storage_config, "confluent_cloud")

        assert mock_create.call_count == 1
        assert result1 is result2

    def test_get_storage_backend_passes_ecosystem(self) -> None:
        from unittest.mock import patch as _patch

        from core.api.dependencies import get_storage_backend

        storage_config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        mock_backend = MagicMock()

        request = MagicMock()
        request.app.state.backends = {}
        tenant_config = MagicMock()
        tenant_config.storage = storage_config
        tenant_config.ecosystem = "confluent_cloud"

        with _patch("core.api.dependencies.get_or_create_backend", return_value=mock_backend) as mock_fn:
            result = get_storage_backend(request, "tenant-a", tenant_config)

        mock_fn.assert_called_once()
        call_args = mock_fn.call_args
        # Fourth positional arg is ecosystem
        passed_ecosystem = call_args.args[3] if len(call_args.args) > 3 else call_args.kwargs.get("ecosystem")
        assert passed_ecosystem == "confluent_cloud"
        assert result is mock_backend
