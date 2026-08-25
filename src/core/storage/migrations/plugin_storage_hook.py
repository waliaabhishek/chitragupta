from __future__ import annotations

from typing import TYPE_CHECKING

from alembic import context, op
from alembic.migration import MigrationContext

from core.plugin.protocols import PluginStorageMigrationModule

if TYPE_CHECKING:
    from sqlalchemy import Connection


PLUGIN_STORAGE_TARGET_REVISION = "033"
CFG_PLUGIN_STORAGE_MODULE = "plugin_storage_module"
CFG_PLUGIN_STORAGE_SELECTION_EXPLICIT = "plugin_storage_selection_explicit"


class PluginStorageSelectionRequiredError(RuntimeError):
    """Raised when a provider selection is required for a plugin downgrade."""


def _selected_module() -> PluginStorageMigrationModule | None:
    attributes = context.config.attributes
    module = attributes.get(CFG_PLUGIN_STORAGE_MODULE)
    if module is None:
        return None
    if not isinstance(module, PluginStorageMigrationModule):
        raise ValueError("plugin storage selection does not provide the required migration capability")
    return module


def run_plugin_storage_step(target_revision: str) -> None:
    """Run selected plugin storage upgrade work for a core revision."""
    module = _selected_module()
    if module is None:
        return
    if context.is_offline_mode():
        raise RuntimeError(
            "selected plugin storage migration requires an online database connection; run the migration without --sql"
        )
    module.prepare_plugin_storage_migration(op.get_bind(), target_revision=target_revision)


def run_plugin_storage_downgrade_step(target_revision: str) -> None:
    """Run selected plugin storage downgrade work before Alembic stamps the core revision."""
    attributes = context.config.attributes
    if not bool(attributes.get(CFG_PLUGIN_STORAGE_SELECTION_EXPLICIT, False)):
        raise PluginStorageSelectionRequiredError(
            "plugin_storage selection is required for downgrade; use "
            "-x plugin_storage=disabled or -x plugin_storage=<ecosystem>"
        )
    module = _selected_module()
    if module is None:
        return
    if context.is_offline_mode():
        raise RuntimeError(
            "selected plugin storage migration requires an online database connection; run the migration without --sql"
        )
    module.downgrade_plugin_storage_migration(op.get_bind(), target_revision=target_revision)


def run_plugin_storage_post_upgrade(connection: Connection, *, target_revision: str) -> None:
    """Prepare selected plugin storage when the core migration is already at its head."""
    module = _selected_module()
    if module is None:
        return
    heads = MigrationContext.configure(connection=connection).get_current_heads()
    if len(heads) != 1:
        raise RuntimeError("plugin storage preparation requires a single Alembic head")
    if heads[0] != target_revision:
        return
    module.prepare_plugin_storage_migration(connection, target_revision=target_revision)


__all__ = [
    "CFG_PLUGIN_STORAGE_MODULE",
    "CFG_PLUGIN_STORAGE_SELECTION_EXPLICIT",
    "PLUGIN_STORAGE_TARGET_REVISION",
    "PluginStorageSelectionRequiredError",
    "run_plugin_storage_downgrade_step",
    "run_plugin_storage_post_upgrade",
    "run_plugin_storage_step",
]
