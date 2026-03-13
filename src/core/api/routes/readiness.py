from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Request

if TYPE_CHECKING:
    from core.config.models import StorageConfig
    from workflow_runner import WorkflowRunner

from core.api import API_VERSION
from core.api.dependencies import get_or_create_backend, get_settings
from core.api.schemas import ReadinessResponse, TenantReadiness
from core.config.models import AppSettings  # noqa: TC001  # FastAPI evaluates annotations at runtime

logger = logging.getLogger(__name__)
router = APIRouter(tags=["readiness"])


def _get_backends(request: Request) -> dict[str, object]:
    if not hasattr(request.app.state, "backends"):
        request.app.state.backends = {}
    return request.app.state.backends  # type: ignore[no-any-return]  # app.state is untyped


def _check_tenant_readiness(
    tenant_name: str,
    ecosystem: str,
    tenant_id: str,
    storage_config: StorageConfig,
    backends: dict[str, object],
    workflow_runner: WorkflowRunner | None,
    failed_tenants: dict[str, str],
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
        backend = get_or_create_backend(backends, tenant_name, storage_config, ecosystem)  # type: ignore[arg-type]  # backends dict is untyped from app.state

        with backend.create_unit_of_work() as uow:
            has_data = uow.pipeline_state.count_calculated(ecosystem, tenant_id) > 0

            latest_run = uow.pipeline_runs.get_latest_run(tenant_name)
            if latest_run is not None:
                last_run_status = latest_run.status
                last_run_at = latest_run.ended_at or latest_run.started_at

                if latest_run.status == "running":
                    # Cross-check: if DB says "running" but workflow_runner disagrees,
                    # the run is orphaned (process restarted). Report as not running.
                    actually_running = (
                        workflow_runner is not None
                        and workflow_runner.is_tenant_running(tenant_name)                    )
                    if actually_running or workflow_runner is None:
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
    """Application readiness check with per-tenant status."""
    mode: str = getattr(request.app.state, "mode", "api")
    workflow_runner = getattr(request.app.state, "workflow_runner", None)
    backends = _get_backends(request)

    failed_tenants: dict[str, str] = {}
    if workflow_runner is not None:
        failed_tenants = workflow_runner.get_failed_tenants()

    tenant_statuses = [
        _check_tenant_readiness(
            tenant_name=name,
            ecosystem=cfg.ecosystem,
            tenant_id=cfg.tenant_id,
            storage_config=cfg.storage,
            backends=backends,
            workflow_runner=workflow_runner,
            failed_tenants=failed_tenants,
        )
        for name, cfg in settings.tenants.items()
    ]

    return ReadinessResponse(
        status=_derive_status(tenant_statuses),
        version=API_VERSION,
        mode=mode,
        tenants=tenant_statuses,
    )
