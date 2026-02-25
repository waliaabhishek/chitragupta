from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request

from core.api.dependencies import get_settings, get_tenant_config
from core.api.schemas import PipelineResultSummary, PipelineRunResponse, PipelineStatusResponse
from core.config.models import AppSettings, TenantConfig  # noqa: TC001  # FastAPI evaluates annotations at runtime

logger = logging.getLogger(__name__)

router = APIRouter(tags=["pipeline"])


@dataclass
class PipelineRunState:
    """Tracks the state of a pipeline run for a tenant."""

    is_running: bool = False
    task: asyncio.Task[None] | None = field(default=None, repr=False)
    last_run: datetime | None = None
    last_result: PipelineResultSummary | None = None


def _get_pipeline_runs(request: Request) -> dict[str, PipelineRunState]:
    """Get or initialize the pipeline_runs dict from app state."""
    if not hasattr(request.app.state, "pipeline_runs"):
        request.app.state.pipeline_runs = {}
    return request.app.state.pipeline_runs  # type: ignore[no-any-return]


async def _run_pipeline(
    tenant_name: str,
    settings: AppSettings,
    runs: dict[str, PipelineRunState],
    request: Request,
) -> None:
    """Background task that runs the pipeline for a single tenant."""
    state = runs[tenant_name]
    try:
        logger.info("Pipeline run started for tenant %s", tenant_name)

        workflow_runner = getattr(request.app.state, "workflow_runner", None)
        if workflow_runner is not None:
            # Real execution via WorkflowRunner
            results = await asyncio.to_thread(workflow_runner.run_once)
            result = results.get(tenant_name)
            if result is not None:
                state.last_result = PipelineResultSummary(
                    dates_gathered=result.dates_gathered,
                    dates_calculated=result.dates_calculated,
                    chargeback_rows_written=result.chargeback_rows_written,
                    errors=result.errors,
                    completed_at=datetime.now(UTC),
                )
            else:
                state.last_result = PipelineResultSummary(
                    dates_gathered=0,
                    dates_calculated=0,
                    chargeback_rows_written=0,
                    errors=[f"Tenant {tenant_name!r} not found in run results"],
                    completed_at=datetime.now(UTC),
                )
        else:
            # TD-039: No WorkflowRunner available (API-only mode).
            # Pipeline trigger is a no-op without a configured plugin registry.
            logger.warning(
                "No WorkflowRunner available for tenant %s — "
                "pipeline trigger requires 'both' mode or a configured plugin registry",
                tenant_name,
            )
            state.last_result = PipelineResultSummary(
                dates_gathered=0,
                dates_calculated=0,
                chargeback_rows_written=0,
                errors=["No WorkflowRunner available — run in 'both' mode to enable pipeline triggers"],
                completed_at=datetime.now(UTC),
            )

        logger.info("Pipeline run completed for tenant %s", tenant_name)
    except Exception:
        state.last_result = PipelineResultSummary(
            dates_gathered=0,
            dates_calculated=0,
            chargeback_rows_written=0,
            errors=["Pipeline execution failed"],
            completed_at=datetime.now(UTC),
        )
        logger.exception("Pipeline run failed for tenant %s", tenant_name)
    finally:
        state.is_running = False
        state.last_run = datetime.now(UTC)
        state.task = None


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
    runs = _get_pipeline_runs(request)

    if tenant_name in runs and runs[tenant_name].is_running:
        raise HTTPException(
            status_code=409,
            detail=f"Pipeline is already running for tenant {tenant_name!r}",
        )

    state = PipelineRunState(is_running=True)
    runs[tenant_name] = state
    state.task = asyncio.create_task(_run_pipeline(tenant_name, settings, runs, request))

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
    runs = _get_pipeline_runs(request)
    state = runs.get(tenant_name)

    if state is None:
        return PipelineStatusResponse(tenant_name=tenant_name, is_running=False, last_run=None, last_result=None)

    return PipelineStatusResponse(
        tenant_name=tenant_name,
        is_running=state.is_running,
        last_run=state.last_run,
        last_result=state.last_result,
    )
