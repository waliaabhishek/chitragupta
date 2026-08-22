from __future__ import annotations

import logging
from collections.abc import Mapping
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

# Import all table modules so they register with SQLModel.metadata
import core.storage.backends.sqlmodel.base_tables  # noqa: F401
import core.storage.backends.sqlmodel.tables  # noqa: F401
import plugins.confluent_cloud.storage.tables  # noqa: F401
import plugins.generic_metrics_only.storage.tables  # noqa: F401
from core.plugin.protocols import PluginStorageMigrationModule
from core.preview.storage_availability import (
    CFG_PREVIEW_EVIDENCE_ENABLED,
    CFG_PREVIEW_EVIDENCE_ISSUES,
    CFG_PREVIEW_EVIDENCE_MODULE,
    PreviewEvidenceIssueCollector,
)
from core.storage.migrations.config import apply_database_url_x_argument
from core.storage.migrations.plugin_storage_hook import (
    CFG_PLUGIN_STORAGE_MODULE,
    CFG_PLUGIN_STORAGE_SELECTION_EXPLICIT,
    PLUGIN_STORAGE_TARGET_REVISION,
    run_plugin_storage_post_upgrade,
)

logger = logging.getLogger(__name__)

target_metadata = SQLModel.metadata

config = context.config
x_arguments = context.get_x_argument(as_dictionary=True)
apply_database_url_x_argument(config, x_arguments)

if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)


def _configure_preview_evidence(x_arguments: Mapping[str, str]) -> None:
    requested = x_arguments.get("focus_preview")
    if requested not in {None, "disabled", "confluent_cloud"}:
        raise ValueError("invalid focus_preview selection; expected 'disabled' or 'confluent_cloud'")
    attributes = config.attributes
    supplied = CFG_PREVIEW_EVIDENCE_ENABLED in attributes
    if supplied:
        enabled = bool(attributes[CFG_PREVIEW_EVIDENCE_ENABLED])
        expected = None if requested is None else requested == "confluent_cloud"
        if expected is not None and enabled != expected:
            raise ValueError("focus_preview selection conflicts with backend configuration")
        attributes.setdefault(CFG_PREVIEW_EVIDENCE_MODULE, None)
        attributes.setdefault(CFG_PREVIEW_EVIDENCE_ISSUES, PreviewEvidenceIssueCollector())
        return
    if requested == "confluent_cloud":
        from core.plugin.protocols import PreviewEvidenceStorageModule
        from plugins.storage_modules import get_storage_module_for_ecosystem

        module = get_storage_module_for_ecosystem("confluent_cloud")
        if not isinstance(module, PreviewEvidenceStorageModule):
            raise ValueError("focus_preview selection does not provide a Preview evidence storage module")
        attributes[CFG_PREVIEW_EVIDENCE_ENABLED] = True
        attributes[CFG_PREVIEW_EVIDENCE_MODULE] = module
    else:
        attributes[CFG_PREVIEW_EVIDENCE_ENABLED] = False
        attributes[CFG_PREVIEW_EVIDENCE_MODULE] = None
    attributes[CFG_PREVIEW_EVIDENCE_ISSUES] = PreviewEvidenceIssueCollector()


def _configure_plugin_storage(x_arguments: Mapping[str, str]) -> None:
    requested = x_arguments.get("plugin_storage")
    attributes = config.attributes
    runtime_supplied = CFG_PLUGIN_STORAGE_MODULE in attributes or CFG_PLUGIN_STORAGE_SELECTION_EXPLICIT in attributes

    if requested is None:
        if not runtime_supplied:
            attributes[CFG_PLUGIN_STORAGE_MODULE] = None
            attributes[CFG_PLUGIN_STORAGE_SELECTION_EXPLICIT] = False
        return

    if not requested.strip():
        raise ValueError("invalid plugin_storage selection; provide 'disabled' or an ecosystem")
    manual_module = None
    if requested != "disabled":
        from plugins.storage_modules import get_storage_module_for_ecosystem

        manual_module = get_storage_module_for_ecosystem(requested)
        if not isinstance(manual_module, PluginStorageMigrationModule):
            raise ValueError("plugin_storage selection does not provide the required migration capability")

    if runtime_supplied and bool(attributes.get(CFG_PLUGIN_STORAGE_SELECTION_EXPLICIT, False)):
        runtime_module = attributes.get(CFG_PLUGIN_STORAGE_MODULE)
        if (runtime_module is None) != (manual_module is None) or (
            runtime_module is not None and manual_module is not None and type(runtime_module) is not type(manual_module)
        ):
            raise ValueError("plugin_storage selection conflicts with backend configuration")

    attributes[CFG_PLUGIN_STORAGE_MODULE] = manual_module
    attributes[CFG_PLUGIN_STORAGE_SELECTION_EXPLICIT] = True


_configure_plugin_storage(x_arguments)
_configure_preview_evidence(x_arguments)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode — emit SQL to script output."""
    if config.attributes.get(CFG_PLUGIN_STORAGE_MODULE) is not None:
        raise RuntimeError(
            "selected plugin storage migration requires an online database connection; run the migration without --sql"
        )
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode — connect to DB and apply."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()
            run_plugin_storage_post_upgrade(connection, target_revision=PLUGIN_STORAGE_TARGET_REVISION)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
