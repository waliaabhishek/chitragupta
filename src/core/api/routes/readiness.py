from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Annotated, Literal

from fastapi import APIRouter, Depends, Request

if TYPE_CHECKING:
    from core.config.models import TenantConfig
    from core.preview.repair import PreviewRepairRuntime
    from core.preview.retention import PreviewRetentionOutcome
    from core.storage.backend_provider import TenantBackendProvider
    from workflow_runner import WorkflowRunner

from core.api import API_VERSION
from core.api.dependencies import get_backend_provider, get_settings
from core.api.schemas import (
    FocusPreviewRetentionDiagnosticResponse,
    FocusPreviewRetentionOutcomeResponse,
    ReadinessResponse,
    TenantReadiness,
)
from core.api.topic_attribution_status import TopicAttributionStatus, resolve_topic_attribution_status
from core.config.models import AppSettings  # noqa: TC001  # FastAPI evaluates annotations at runtime

logger = logging.getLogger(__name__)
router = APIRouter(tags=["readiness"])

# Module-level TTL cache for readiness responses.
# Prevents N concurrent polls from each hitting the DB.
# 2s TTL is safe given 5s polling interval.
_readiness_cache: tuple[ReadinessResponse, float] | None = None
_READINESS_CACHE_TTL: float = 2.0  # seconds

_PREVIEW_DISABLED = "FOCUS Mapping Preview is not enabled for this tenant."
_PREVIEW_UNAVAILABLE = "FOCUS Mapping Preview storage is unavailable. Restore storage availability before retrying."
_PREVIEW_UPGRADING = "Historical repair is in progress; existing valid Preview data remains available."
_PREVIEW_DEGRADED = (
    "Historical repair needs attention. Retry the failed dates with a new bounded repair; "
    "existing valid Preview data remains available."
)
_PREVIEW_RETENTION_DEGRADED = (
    "Retention cleanup needs attention. Review the latest retention outcome and worker logs; "
    "existing valid Preview data remains available."
)
_PREVIEW_REPAIR_AND_RETENTION_DEGRADED = (
    "Historical repair and retention cleanup need attention. Retry failed repair dates after "
    "retention cleanup succeeds; existing valid Preview data remains available."
)
_PREVIEW_REPAIR_RUNNING_AND_RETENTION_DEGRADED = (
    "Retention cleanup needs attention while historical repair is in progress. "
    "Existing valid Preview data remains available."
)


@dataclass(frozen=True)
class FocusPreviewReadiness:
    state: Literal["disabled", "ready", "upgrading", "degraded", "unavailable"]
    completed_repair_dates: int | None
    total_repair_dates: int | None
    message: str | None
    ordinary_retention: PreviewRetentionOutcome | None
    evidence_retention: PreviewRetentionOutcome | None


def _unavailable_preview_readiness() -> FocusPreviewReadiness:
    return FocusPreviewReadiness(
        state="unavailable",
        completed_repair_dates=None,
        total_repair_dates=None,
        message=_PREVIEW_UNAVAILABLE,
        ordinary_retention=None,
        evidence_retention=None,
    )


def _focus_preview_readiness(
    *,
    tenant_config: TenantConfig,
    backend: object | None,
    recovery_available: bool | None,
) -> FocusPreviewReadiness:
    from core.preview.persistence import PreviewEvidenceStorageBackend
    from core.preview.repair import PreviewRepairHistoryUnresolved, PreviewRepairStatus
    from core.preview.retention import PreviewRetentionOutcomeStatus
    from core.preview.storage_availability import PreviewEvidenceAvailabilityState

    if not tenant_config.focus_preview_enabled:
        return FocusPreviewReadiness(
            state="disabled",
            completed_repair_dates=None,
            total_repair_dates=None,
            message=_PREVIEW_DISABLED,
            ordinary_retention=None,
            evidence_retention=None,
        )
    unavailable = _unavailable_preview_readiness()
    if recovery_available is False or not isinstance(backend, PreviewEvidenceStorageBackend):
        return unavailable
    if backend.preview_evidence_availability.state is not PreviewEvidenceAvailabilityState.READY:
        return unavailable
    try:
        with backend.create_preview_generation_read_unit_of_work() as uow:
            progress = uow.repairs.get_current_progress_for_owner(
                tenant_config.ecosystem,
                tenant_config.tenant_id,
            )
            outcomes = uow.retention_outcomes.get_latest_for_owner(
                tenant_config.ecosystem,
                tenant_config.tenant_id,
            )
    except Exception:
        return unavailable
    if isinstance(progress, PreviewRepairHistoryUnresolved):
        return unavailable

    completed_dates = None if progress is None else progress.completed_dates
    total_dates = None if progress is None else progress.total_dates
    retention_failed = any(
        outcome is not None and outcome.status is PreviewRetentionOutcomeStatus.FAILURE
        for outcome in (outcomes.ordinary, outcomes.preview_evidence)
    )
    if retention_failed:
        if progress is not None and progress.status in {
            PreviewRepairStatus.QUEUED,
            PreviewRepairStatus.RUNNING,
        }:
            message = _PREVIEW_REPAIR_RUNNING_AND_RETENTION_DEGRADED
        elif progress is not None and progress.status is not PreviewRepairStatus.COMPLETED:
            message = _PREVIEW_REPAIR_AND_RETENTION_DEGRADED
        else:
            message = _PREVIEW_RETENTION_DEGRADED
        return FocusPreviewReadiness(
            state="degraded",
            completed_repair_dates=completed_dates,
            total_repair_dates=total_dates,
            message=message,
            ordinary_retention=outcomes.ordinary,
            evidence_retention=outcomes.preview_evidence,
        )
    if progress is None:
        return FocusPreviewReadiness(
            state="ready",
            completed_repair_dates=None,
            total_repair_dates=None,
            message=None,
            ordinary_retention=outcomes.ordinary,
            evidence_retention=outcomes.preview_evidence,
        )
    if progress.status in {PreviewRepairStatus.QUEUED, PreviewRepairStatus.RUNNING}:
        return FocusPreviewReadiness(
            state="upgrading",
            completed_repair_dates=progress.completed_dates,
            total_repair_dates=progress.total_dates,
            message=_PREVIEW_UPGRADING,
            ordinary_retention=outcomes.ordinary,
            evidence_retention=outcomes.preview_evidence,
        )
    if progress.status is PreviewRepairStatus.COMPLETED:
        return FocusPreviewReadiness(
            state="ready",
            completed_repair_dates=progress.completed_dates,
            total_repair_dates=progress.total_dates,
            message=None,
            ordinary_retention=outcomes.ordinary,
            evidence_retention=outcomes.preview_evidence,
        )
    return FocusPreviewReadiness(
        state="degraded",
        completed_repair_dates=progress.completed_dates,
        total_repair_dates=progress.total_dates,
        message=_PREVIEW_DEGRADED,
        ordinary_retention=outcomes.ordinary,
        evidence_retention=outcomes.preview_evidence,
    )


def _retention_outcome_response(
    outcome: PreviewRetentionOutcome | None,
) -> FocusPreviewRetentionOutcomeResponse | None:
    if outcome is None:
        return None
    diagnostic = (
        None
        if outcome.diagnostic is None
        else FocusPreviewRetentionDiagnosticResponse(
            code=outcome.diagnostic.code,
            message=outcome.diagnostic.message,
            error_type=outcome.diagnostic.error_type,
        )
    )
    return FocusPreviewRetentionOutcomeResponse(
        attempted_at=outcome.attempted_at,
        status=outcome.status.value,
        diagnostic=diagnostic,
    )


def _check_tenant_readiness(
    tenant_name: str,
    tenant_config: TenantConfig,
    backend_provider: TenantBackendProvider,
    workflow_runner: WorkflowRunner | None,
    failed_tenants: dict[str, str],
    topic_attribution_status: TopicAttributionStatus,
    repair_runtime: PreviewRepairRuntime | None = None,
) -> TenantReadiness:
    """Check readiness for a single tenant. Pure function over injected dependencies."""
    tables_ready = True
    has_data = False
    pipeline_running = False
    pipeline_stage: str | None = None
    pipeline_current_date: date | None = None
    last_run_status: str | None = None
    last_run_at = None
    permanent_failure = failed_tenants.get(tenant_name)

    try:
        with (
            backend_provider.acquire_backend(tenant_name, tenant_config) as backend,
            backend.create_read_only_unit_of_work() as uow,
        ):
            has_data = uow.pipeline_state.count_calculated(tenant_config.ecosystem, tenant_config.tenant_id) > 0

            latest_run = uow.pipeline_runs.get_latest_run(tenant_name)
            if latest_run is not None:
                last_run_status = latest_run.status
                last_run_at = latest_run.ended_at or latest_run.started_at

                if latest_run.status == "running":
                    # Cross-check: if DB says "running" but workflow_runner disagrees,
                    # the run is orphaned (process restarted). Report as not running.
                    if workflow_runner is None:
                        last_run_status = "failed"
                    else:
                        actually_running = workflow_runner.is_tenant_running(tenant_name)
                        if actually_running:
                            pipeline_running = True
                            pipeline_stage = latest_run.stage
                            pipeline_current_date = latest_run.current_date
                        else:
                            last_run_status = "failed"

        # Also check workflow_runner for in-progress runs not yet in DB
        if workflow_runner is not None and not pipeline_running:
            pipeline_running = workflow_runner.is_tenant_running(tenant_name)
    except Exception:
        logger.warning("Failed to check readiness for tenant %s", tenant_name, exc_info=True)
        tables_ready = False

    if not tenant_config.focus_preview_enabled:
        preview_state = _focus_preview_readiness(
            tenant_config=tenant_config,
            backend=None,
            recovery_available=None,
        )
    elif not tables_ready:
        preview_state = _unavailable_preview_readiness()
    else:
        try:
            with backend_provider.acquire_backend(tenant_name, tenant_config) as backend:
                preview_state = _focus_preview_readiness(
                    tenant_config=tenant_config,
                    backend=backend,
                    recovery_available=(
                        None if repair_runtime is None else repair_runtime.recovery_available(tenant_name)
                    ),
                )
        except Exception as exc:
            logger.warning(
                "Failed to check FOCUS Preview readiness tenant=%s error_type=%s",
                tenant_name,
                type(exc).__name__,
            )
            preview_state = _unavailable_preview_readiness()

    return TenantReadiness(
        tenant_name=tenant_name,
        tables_ready=tables_ready,
        has_data=has_data,
        pipeline_running=pipeline_running,
        pipeline_stage=pipeline_stage,
        pipeline_current_date=pipeline_current_date,
        last_run_status=last_run_status,
        last_run_at=last_run_at,
        permanent_failure=permanent_failure,
        topic_attribution_status=topic_attribution_status.status,
        topic_attribution_error=topic_attribution_status.error,
        focus_preview_state=preview_state.state,
        focus_preview_completed_repair_dates=preview_state.completed_repair_dates,
        focus_preview_total_repair_dates=preview_state.total_repair_dates,
        focus_preview_message=preview_state.message,
        focus_preview_ordinary_retention=_retention_outcome_response(preview_state.ordinary_retention),
        focus_preview_evidence_retention=_retention_outcome_response(preview_state.evidence_retention),
    )


def _derive_status(tenants: list[TenantReadiness]) -> str:
    """Derive top-level application status from per-tenant readiness."""
    if any(not t.tables_ready for t in tenants):
        return "initializing"
    if tenants and all(t.permanent_failure is not None for t in tenants):
        return "error"
    if any(t.has_data for t in tenants):
        return "ready"
    return "no_data"


@router.get("/readiness", response_model=ReadinessResponse)
def readiness(
    request: Request,
    settings: Annotated[AppSettings, Depends(get_settings)],
) -> ReadinessResponse:
    """Application readiness check with per-tenant status. TTL-cached for 2s."""
    global _readiness_cache  # noqa: PLW0603

    now = time.monotonic()
    if _readiness_cache is not None and now - _readiness_cache[1] < _READINESS_CACHE_TTL:
        return _readiness_cache[0]

    mode: str = getattr(request.app.state, "mode", "api")
    workflow_runner = getattr(request.app.state, "workflow_runner", None)
    backend_provider = get_backend_provider(request)
    repair_runtime = getattr(request.app.state, "preview_repair_runtime", None)

    failed_tenants: dict[str, str] = {}
    if workflow_runner is not None:
        failed_tenants = workflow_runner.get_failed_tenants()

    tenant_statuses = [
        _check_tenant_readiness(
            tenant_name=name,
            tenant_config=cfg,
            backend_provider=backend_provider,
            workflow_runner=workflow_runner,
            failed_tenants=failed_tenants,
            topic_attribution_status=resolve_topic_attribution_status(cfg.plugin_settings, cfg.ecosystem),
            repair_runtime=repair_runtime,
        )
        for name, cfg in settings.tenants.items()
    ]

    result = ReadinessResponse(
        status=_derive_status(tenant_statuses),
        version=API_VERSION,
        mode=mode,
        tenants=tenant_statuses,
    )
    _readiness_cache = (result, now)
    return result
