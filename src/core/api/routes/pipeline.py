from __future__ import annotations

import asyncio
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from core.api.dependencies import get_or_create_backend, get_tenant_config
from core.api.schemas import PipelineResultSummary, PipelineRunResponse, PipelineStatusResponse
from core.config.models import TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime
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
    request: Request,
) -> None:
    """Background task: delegates pipeline execution to WorkflowRunner.

    PipelineRun lifecycle is owned by WorkflowRunner._run_tenant() via PipelineRunTracker.
    This function is a thin async wrapper that handles thread dispatch and logging only.
    """
    try:
        logger.info("Pipeline run started for tenant %s", tenant_name)
        workflow_runner = getattr(request.app.state, "workflow_runner", None)
        if workflow_runner is None:
            logger.error(
                "Pipeline background task for %s: no WorkflowRunner (should have been rejected at endpoint)",
                tenant_name,
            )
            return

        result = await asyncio.to_thread(workflow_runner.run_tenant, tenant_name)

        if result.already_running:
            logger.info("Pipeline run skipped for tenant %s — already in progress", tenant_name)
        else:
            logger.info("Pipeline run completed for tenant %s", tenant_name)
    except Exception:
        logger.exception("Pipeline run failed for tenant %s", tenant_name)


@router.post(
    "/tenants/{tenant_name}/pipeline/run",
    response_model=PipelineRunResponse,
    status_code=202,
)
async def trigger_pipeline(
    request: Request,
    response: Response,
    tenant_name: str,
    tenant_config: Annotated[TenantConfig, Depends(get_tenant_config)],
) -> PipelineRunResponse:
    tasks = _get_pipeline_tasks(request)

    if tenant_name in tasks and not tasks[tenant_name].done():
        raise HTTPException(
            status_code=409,
            detail=f"Pipeline is already running for tenant {tenant_name!r}",
        )

    workflow_runner = getattr(request.app.state, "workflow_runner", None)
    if workflow_runner is None:
        raise HTTPException(
            status_code=400,
            detail="Pipeline trigger requires 'both' mode — no WorkflowRunner is configured",
        )

    if workflow_runner.is_tenant_running(tenant_name):
        response.status_code = 200
        return PipelineRunResponse(
            tenant_name=tenant_name,
            status="already_running",
            message=f"Tenant {tenant_name!r} is already being processed",
        )

    task = asyncio.create_task(_run_pipeline(tenant_name, request))
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
    backend = get_or_create_backend(_get_backends(request), tenant_name, tenant_config.storage, tenant_config.ecosystem)

    with backend.create_read_only_unit_of_work() as uow:
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
