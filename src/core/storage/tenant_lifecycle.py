from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from core.logging_context import safe_exception_context, safe_log_context
from core.preview.eligibility import policy_from_tenant_config
from core.preview.persistence import PreviewEvidenceStorageBackend
from core.preview.storage_availability import (
    PreviewEvidenceAvailabilityState,
    PreviewEvidenceBootstrapUnavailable,
)

if TYPE_CHECKING:
    from core.config.models import TenantConfig
    from core.preview.evidence import PreviewEvidenceBootstrapResult
    from core.storage.interface import StorageBackend

logger = logging.getLogger(__name__)


def cleanup_orphaned_pipeline_run(storage: StorageBackend, tenant_name: str) -> None:
    """Best-effort cleanup of the latest run left active by a prior process."""
    try:
        with storage.create_unit_of_work() as uow:
            latest = uow.pipeline_runs.get_latest_run(tenant_name)
            if latest is None or latest.status != "running":
                return
            latest.status = "failed"
            latest.ended_at = datetime.now(UTC)
            latest.stage = None
            latest.current_date = None
            latest.error_message = "Orphaned — process restarted before completion"
            uow.pipeline_runs.update_run(latest)
            uow.commit()
            logger.info(
                "Cleaned up orphaned 'running' PipelineRun for tenant %s (id=%s)",
                tenant_name,
                latest.id,
            )
    except Exception as exc:
        logger.warning(
            "orphaned_pipeline_cleanup_failed%s",
            safe_log_context(
                tenant_name=tenant_name,
                stage="pipeline_run_cleanup",
                outcome="failed",
                retryable=True,
                **safe_exception_context(exc),
            ),
        )


def prepare_tenant_backend(
    storage: StorageBackend,
    tenant_name: str,
    config: TenantConfig,
) -> PreviewEvidenceBootstrapResult | PreviewEvidenceBootstrapUnavailable | None:
    """Complete one newly constructed tenant backend before it is published."""
    storage.create_tables()
    bootstrap_result: PreviewEvidenceBootstrapResult | PreviewEvidenceBootstrapUnavailable | None = None
    if (
        config.focus_preview_enabled
        and isinstance(storage, PreviewEvidenceStorageBackend)
        and storage.preview_evidence_availability.state is PreviewEvidenceAvailabilityState.READY
    ):
        try:
            preview_policy = policy_from_tenant_config(
                config,
                created_at=datetime.now(UTC),
            )
            assert preview_policy.effective_start_date is not None
            assert preview_policy.effective_end_date is not None
            bootstrap_result = storage.create_preview_evidence_bootstrap().bootstrap_owner(
                ecosystem=config.ecosystem,
                tenant_id=config.tenant_id,
                policy_start=datetime.combine(
                    preview_policy.effective_start_date,
                    datetime.min.time(),
                    tzinfo=UTC,
                ),
                policy_end=datetime.combine(
                    preview_policy.effective_end_date,
                    datetime.min.time(),
                    tzinfo=UTC,
                ),
            )
        except Exception as exc:
            error_type = type(exc).__name__
            storage.mark_preview_evidence_bootstrap_unavailable(error_type)
            bootstrap_result = PreviewEvidenceBootstrapUnavailable(error_type)
            logger.warning(
                "Preview evidence bootstrap unavailable%s",
                safe_log_context(
                    tenant_name=tenant_name,
                    tenant_id=config.tenant_id,
                    stage="preview_evidence_bootstrap",
                    outcome="unavailable",
                    retryable=True,
                    **safe_exception_context(exc),
                ),
            )
    cleanup_orphaned_pipeline_run(storage, tenant_name)
    return bootstrap_result
