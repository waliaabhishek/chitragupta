from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from core.config.models import StorageConfig
from core.storage.interface import StorageBackend


class TestStorageBackendRegistry:
    def test_registry_registers_and_creates(self) -> None:
        from core.storage.registry import StorageBackendRegistry

        registry = StorageBackendRegistry()
        mock_factory = MagicMock(return_value=MagicMock())
        registry.register("sqlmodel", mock_factory)

        backend = registry.create("sqlmodel", "sqlite:///:memory:", use_migrations=True)

        mock_factory.assert_called_once_with("sqlite:///:memory:", True)
        assert backend is mock_factory.return_value

    def test_duplicate_registration_raises(self) -> None:
        from core.storage.registry import StorageBackendRegistry

        registry = StorageBackendRegistry()
        registry.register("sqlmodel", lambda cs, um: MagicMock())

        with pytest.raises(ValueError, match="sqlmodel"):
            registry.register("sqlmodel", lambda cs, um: MagicMock())

    def test_unknown_backend_raises_key_error(self) -> None:
        from core.storage.registry import StorageBackendRegistry

        registry = StorageBackendRegistry()

        with pytest.raises(KeyError, match="unknown"):
            registry.create("unknown", "sqlite:///:memory:", use_migrations=False)

    def test_create_passes_use_migrations_false(self) -> None:
        from core.storage.registry import StorageBackendRegistry

        registry = StorageBackendRegistry()
        mock_factory = MagicMock(return_value=MagicMock())
        registry.register("sqlmodel", mock_factory)

        registry.create("sqlmodel", "sqlite:///:memory:", use_migrations=False)

        mock_factory.assert_called_once_with("sqlite:///:memory:", False)

    def test_list_backends_returns_registered_names(self) -> None:
        from core.storage.registry import StorageBackendRegistry

        registry = StorageBackendRegistry()
        registry.register("a", lambda cs, um: MagicMock())
        registry.register("b", lambda cs, um: MagicMock())
        assert set(registry.list_backends()) == {"a", "b"}


class TestCreateStorageBackendFunction:
    def test_routes_through_default_registry(self) -> None:
        from core.storage import registry as registry_module

        config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        mock_backend = MagicMock()

        with patch.object(
            registry_module._default_storage_registry, "create", return_value=mock_backend
        ) as mock_create:
            result = registry_module.create_storage_backend(config)

        mock_create.assert_called_once_with("sqlmodel", "sqlite:///:memory:", use_migrations=True)
        assert result is mock_backend

    def test_passes_use_migrations_false(self) -> None:
        from core.storage import registry as registry_module
        from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

        config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")

        result = registry_module.create_storage_backend(config, use_migrations=False)

        assert isinstance(result, SQLModelBackend)
        result.dispose()

    def test_default_use_migrations_is_true(self) -> None:
        from core.storage import registry as registry_module

        config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        mock_backend = MagicMock()

        with patch.object(
            registry_module._default_storage_registry, "create", return_value=mock_backend
        ) as mock_create:
            registry_module.create_storage_backend(config)

        call_args = mock_create.call_args
        assert call_args.kwargs.get("use_migrations") is True


class TestWorkflowRunnerNoPrivateFunction:
    def test_create_storage_backend_removed_from_workflow_runner(self) -> None:
        import workflow_runner

        assert not hasattr(workflow_runner, "_create_storage_backend"), (
            "_create_storage_backend should not exist in workflow_runner after TASK-007"
        )


class TestGetOrCreateBackendAcceptsStorageConfig:
    @patch("core.api.dependencies.create_storage_backend")
    def test_respects_storage_config_backend(self, mock_create: MagicMock) -> None:
        from core.api.dependencies import get_or_create_backend

        mock_backend = MagicMock()
        mock_create.return_value = mock_backend

        storage_config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        backends: dict[str, StorageBackend] = {}

        result = get_or_create_backend(backends, "tenant-a", storage_config)

        mock_create.assert_called_once_with(storage_config, use_migrations=False)
        assert result is mock_backend

    @patch("core.api.dependencies.create_storage_backend")
    def test_caches_backend_per_tenant(self, mock_create: MagicMock) -> None:
        from core.api.dependencies import get_or_create_backend

        mock_backend = MagicMock()
        mock_create.return_value = mock_backend

        storage_config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        backends: dict[str, StorageBackend] = {}

        result1 = get_or_create_backend(backends, "tenant-a", storage_config)
        result2 = get_or_create_backend(backends, "tenant-a", storage_config)

        assert mock_create.call_count == 1
        assert result1 is result2

    def test_get_storage_backend_passes_storage_config(self) -> None:
        from unittest.mock import patch as _patch

        from core.api.dependencies import get_storage_backend

        storage_config = StorageConfig(backend="sqlmodel", connection_string="sqlite:///:memory:")
        mock_backend = MagicMock()

        request = MagicMock()
        request.app.state.backends = {}
        tenant_config = MagicMock()
        tenant_config.storage = storage_config

        with _patch("core.api.dependencies.get_or_create_backend", return_value=mock_backend) as mock_fn:
            result = get_storage_backend(request, "tenant-a", tenant_config)

        mock_fn.assert_called_once()
        call_args = mock_fn.call_args
        # Third positional arg (or keyword arg storage_config) must be the StorageConfig object
        passed_config = call_args.args[2] if len(call_args.args) > 2 else call_args.kwargs.get("storage_config")
        assert passed_config is storage_config
        assert result is mock_backend
