from __future__ import annotations

import logging

from alembic import context, op
from sqlalchemy.exc import SQLAlchemyError

from core.plugin.protocols import PreviewEvidenceStorageModule
from core.preview.storage_availability import (
    CFG_PREVIEW_EVIDENCE_ENABLED,
    CFG_PREVIEW_EVIDENCE_ISSUES,
    CFG_PREVIEW_EVIDENCE_MODULE,
    PreviewEvidenceIssue,
    PreviewEvidenceIssueCollector,
    PreviewEvidenceIssueKind,
    PreviewEvidenceOfflineMigrationError,
    PreviewEvidenceSchemaError,
)

logger = logging.getLogger(__name__)

_ONLINE_ENABLED_UPGRADE_COMMAND = (
    "uv run alembic -c src/core/storage/migrations/alembic.ini -x focus_preview=confluent_cloud upgrade head"
)
_DOWNGRADE_TARGETS = {
    "030": "029",
    "028": "027",
    "027": "026",
    "026": "025",
    "021": "020",
    "018": "017",
}


def run_preview_evidence_step(target_revision: str) -> None:
    attributes = context.config.attributes
    if not bool(attributes.get(CFG_PREVIEW_EVIDENCE_ENABLED, False)):
        return
    module = attributes.get(CFG_PREVIEW_EVIDENCE_MODULE)
    collector = attributes.get(CFG_PREVIEW_EVIDENCE_ISSUES)
    if not isinstance(module, PreviewEvidenceStorageModule):
        raise ValueError("focus_preview enabled without a Preview evidence storage module")
    if not isinstance(collector, PreviewEvidenceIssueCollector):
        raise ValueError("focus_preview enabled without an evidence issue collector")
    if context.is_offline_mode():
        raise PreviewEvidenceOfflineMigrationError(
            "Preview evidence migrations require an online database connection; run "
            f"`{_ONLINE_ENABLED_UPGRADE_COMMAND}`."
        )
    connection = op.get_bind()
    try:
        if connection.dialect.name == "postgresql":
            # PostgreSQL aborts the containing transaction after failed DDL.
            # Isolate optional Preview work so the savepoint is rolled back
            # before the sanitized availability issue is recorded and ignored.
            with connection.begin_nested():
                module.prepare_preview_evidence_migration(
                    connection,
                    target_revision=target_revision,
                )
        else:
            module.prepare_preview_evidence_migration(
                connection,
                target_revision=target_revision,
            )
    except (PreviewEvidenceSchemaError, SQLAlchemyError) as exc:
        collector.record(
            PreviewEvidenceIssue(
                revision=target_revision,
                kind=(
                    PreviewEvidenceIssueKind.SCHEMA_INCOMPATIBLE
                    if isinstance(exc, PreviewEvidenceSchemaError)
                    else PreviewEvidenceIssueKind.DDL_FAILED
                ),
                error_type=type(exc).__name__,
            )
        )


def run_preview_evidence_downgrade_step(target_revision: str) -> None:
    if context.is_offline_mode():
        downgrade_target = _DOWNGRADE_TARGETS.get(target_revision)
        if downgrade_target is None:
            raise ValueError(f"unsupported Preview evidence downgrade revision: {target_revision}")
        command = (
            "uv run alembic -c src/core/storage/migrations/alembic.ini "
            f"-x focus_preview=confluent_cloud downgrade {downgrade_target}"
        )
        raise PreviewEvidenceOfflineMigrationError(
            f"Preview evidence downgrades require an online database connection; run `{command}`."
        )
    from plugins.storage_modules import get_storage_module_for_ecosystem

    module = get_storage_module_for_ecosystem("confluent_cloud")
    if not isinstance(module, PreviewEvidenceStorageModule):
        raise ValueError("Confluent Cloud lacks a Preview evidence storage module")
    module.downgrade_preview_evidence_migration(
        op.get_bind(),
        target_revision=target_revision,
    )
