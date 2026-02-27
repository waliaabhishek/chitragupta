from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request

from core.api.dependencies import get_or_create_backend, get_settings, get_tenant_config
from core.api.schemas import PipelineResultSummary, PipelineRunResponse, PipelineStatusResponse
from core.config.models import AppSettings, TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
from core.storage.interface import StorageBackend  # noqa: TC001

logger = logging.getLogger(__name__)

router = APIRouter(tags=["pipeline"])


def _get_pipeline_tasks(request: Request) -> dict[str, asyncio.Task[None]]:
    """Get or initialize the in-memory task tracking dict."""
    if not hasattr(request.app.state, "pipeline_tasks"):
        request.app.state.pipeline_tasks = {}
    return request.app.state.pipeline_tasks  # type: ignore[no-any-return]  # app.state is untyped Starlette state dict


def _get_backends(request: Request) -> dict[str, StorageBackend]:
    if not hasattr(request.app.state, "backends"):
        request.app.state.backends = {}
    return request.app.state.backends  # type: ignore[no-any-return]  # app.state is untyped Starlette state dict


async def _run_pipeline(
    tenant_name: str,
    settings: AppSettings,
    run_id: int,
    backend: StorageBackend,
    request: Request,
) -> None:
    """Background task that runs the pipeline for a single tenant and persists results."""
    try:
        logger.info("Pipeline run started for tenant %s", tenant_name)

        workflow_runner = getattr(request.app.state, "workflow_runner", None)
        if workflow_runner is not None:
            # TD-039: Run single tenant instead of all tenants
            result = await asyncio.to_thread(workflow_runner.run_tenant, tenant_name)
            dates_gathered = result.dates_gathered
            dates_calculated = result.dates_calculated
            rows_written = result.chargeback_rows_written
            errors = result.errors
        else:
            # TD-039: No WorkflowRunner available (API-only mode).
            logger.warning(
                "No WorkflowRunner available for tenant %s — "
                "pipeline trigger requires 'both' mode or a configured plugin registry",
                tenant_name,
            )
            dates_gathered = 0
            dates_calculated = 0
            rows_written = 0
            errors = ["No WorkflowRunner available — run in 'both' mode to enable pipeline triggers"]

        with backend.create_unit_of_work() as uow:
            run = uow.pipeline_runs.get_run(run_id)
            if run is not None:
                run.status = "completed"
                run.ended_at = datetime.now(UTC)
                run.dates_gathered = dates_gathered
                run.dates_calculated = dates_calculated
                run.rows_written = rows_written
                run.error_message = errors[0] if errors else None
                uow.pipeline_runs.update_run(run)
                uow.commit()

        logger.info("Pipeline run completed for tenant %s", tenant_name)
    except Exception:
        logger.exception("Pipeline run failed for tenant %s", tenant_name)
        try:
            with backend.create_unit_of_work() as uow:
                run = uow.pipeline_runs.get_run(run_id)
                if run is not None:
                    run.status = "failed"
                    run.ended_at = datetime.now(UTC)
                    run.error_message = "Pipeline execution failed"
                    uow.pipeline_runs.update_run(run)
                    uow.commit()
        except Exception:
            logger.exception("Failed to persist pipeline failure state for tenant %s", tenant_name)
            raise


@router.post(
    "/tenants/{tenant_name}/pipeline/run",
    response_model=PipelineRunResponse,
    status_code=202,
)
async def trigger_pipeline(
    request: Request,
    tenant_name: str,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    settings: Annotated[AppSettings, Depends(get_settings)],
) -> PipelineRunResponse:
    tasks = _get_pipeline_tasks(request)

    if tenant_name in tasks and not tasks[tenant_name].done():
        raise HTTPException(
            status_code=409,
            detail=f"Pipeline is already running for tenant {tenant_name!r}",
        )

    backend = get_or_create_backend(_get_backends(request), tenant_name, tenant_config.storage.connection_string)

    with backend.create_unit_of_work() as uow:
        run = uow.pipeline_runs.create_run(tenant_name, datetime.now(UTC))
        uow.commit()

    task = asyncio.create_task(_run_pipeline(tenant_name, settings, run.id, backend, request))  # type: ignore[arg-type]  # id is set after flush
    tasks[tenant_name] = task

    return PipelineRunResponse(
        tenant_name=tenant_name,
        status="started",
        message=f"Pipeline run started for tenant {tenant_name!r}",
    )


@router.get(
    "/tenants/{tenant_name}/pipeline/status",
    response_model=PipelineStatusResponse,
)
async def pipeline_status(
    request: Request,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
    tenant_name: str,
) -> PipelineStatusResponse:
    backend = get_or_create_backend(_get_backends(request), tenant_name, tenant_config.storage.connection_string)

    with backend.create_unit_of_work() as uow:
        latest = uow.pipeline_runs.get_latest_run(tenant_name)

    if latest is None:
        return PipelineStatusResponse(tenant_name=tenant_name, is_running=False, last_run=None, last_result=None)

    is_running = latest.status == "running"
    last_run = latest.ended_at or latest.started_at
    last_result: PipelineResultSummary | None = None

    if latest.status in ("completed", "failed"):
        errors = [latest.error_message] if latest.error_message else []
        last_result = PipelineResultSummary(
            dates_gathered=latest.dates_gathered,
            dates_calculated=latest.dates_calculated,
            chargeback_rows_written=latest.rows_written,
            errors=errors,
            completed_at=latest.ended_at or latest.started_at,
        )

    return PipelineStatusResponse(
        tenant_name=tenant_name,
        is_running=is_running,
        last_run=last_run,
        last_result=last_result,
    )
