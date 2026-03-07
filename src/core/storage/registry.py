from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.config.models import StorageConfig
    from core.plugin.protocols import StorageModule
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)


def create_storage_backend(
    config: StorageConfig,
    *,
    storage_module: StorageModule | None = None,
    use_migrations: bool = True,
) -> StorageBackend:
    """Create a storage backend from config.

    Args:
        config: Storage configuration (backend type, connection string).
        storage_module: Plugin-specific storage module. If None, uses CoreStorageModule.
        use_migrations: Whether to run Alembic migrations on table creation.

    Returns:
        Configured StorageBackend instance.
    """
    if config.backend != "sqlmodel":
        raise ValueError(f"Unknown storage backend: {config.backend!r}")

    from core.storage.backends.sqlmodel.module import CoreStorageModule
    from core.storage.backends.sqlmodel.unit_of_work import SQLModelBackend

    module = storage_module if storage_module is not None else CoreStorageModule()
    logger.info(
        "Creating storage backend=%r storage_module=%s use_migrations=%s",
        config.backend,
        type(module).__name__,
        use_migrations,
    )
    return SQLModelBackend(config.connection_string, module, use_migrations=use_migrations)
