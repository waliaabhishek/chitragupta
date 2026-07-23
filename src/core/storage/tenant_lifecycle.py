from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

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
    except Exception:
        logger.warning("Failed to clean up orphaned runs for %s", tenant_name, exc_info=True)


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
            policy = config.focus_preview
            assert policy is not None
            bootstrap_result = storage.create_preview_evidence_bootstrap().bootstrap_owner(
                ecosystem=config.ecosystem,
                tenant_id=config.tenant_id,
                policy_start=datetime.combine(policy.effective_start_date, datetime.min.time(), tzinfo=UTC),
                policy_end=datetime.combine(policy.effective_end_date, datetime.min.time(), tzinfo=UTC),
            )
        except Exception as exc:
            error_type = type(exc).__name__
            storage.mark_preview_evidence_bootstrap_unavailable(error_type)
            bootstrap_result = PreviewEvidenceBootstrapUnavailable(error_type)
            logger.warning(
                "Preview evidence bootstrap unavailable tenant=%s error_type=%s",
                tenant_name,
                error_type,
            )
    cleanup_orphaned_pipeline_run(storage, tenant_name)
    return bootstrap_result
