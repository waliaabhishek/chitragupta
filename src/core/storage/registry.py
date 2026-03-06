from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.config.models import StorageConfig
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)

# Factory type: (connection_string, use_migrations) -> StorageBackend
BackendFactory = Callable[[str, bool], "StorageBackend"]


class StorageBackendRegistry:
    """Factory lookup: backend name -> backend factory. Stores no initialized state."""

    _factories: dict[str, BackendFactory]

    def __init__(self) -> None:
        self._factories = {}

    def register(self, backend: str, factory: BackendFactory) -> None:
        logger.debug("Registering storage backend %r", backend)
        if backend in self._factories:
            raise ValueError(f"Backend '{backend}' is already registered")
        self._factories[backend] = factory

    def create(self, backend: str, connection_string: str, *, use_migrations: bool) -> StorageBackend:
        logger.info("Creating storage backend=%r use_migrations=%s", backend, use_migrations)
        try:
            factory = self._factories[backend]
        except KeyError:
            logger.exception("Unknown storage backend=%r known=%s", backend, list(self._factories))
            raise KeyError(f"Unknown storage backend: {backend!r}") from None
        backend_instance = factory(connection_string, use_migrations)
        logger.info("Storage backend %r created", backend)
        return backend_instance

    def list_backends(self) -> list[str]:
        return list(self._factories)


def _sqlmodel_factory(connection_string: str, use_migrations: bool) -> StorageBackend:
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

    return SQLModelBackend(connection_string, use_migrations=use_migrations)


_default_storage_registry = StorageBackendRegistry()
_default_storage_registry.register("sqlmodel", _sqlmodel_factory)


def create_storage_backend(config: StorageConfig, *, use_migrations: bool = True) -> StorageBackend:
    """Create a storage backend from config using the default registry."""
    return _default_storage_registry.create(config.backend, config.connection_string, use_migrations=use_migrations)
